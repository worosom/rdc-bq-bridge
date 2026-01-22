"""Redis listener implementations for client-side tracking and channel subscriptions."""

import asyncio
import hashlib
import logging
from typing import Any, Dict, Optional

import redis.asyncio as redis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import RoutingRule
from .listener_base import RedisListenerBase

logger = logging.getLogger(__name__)


class ClientSideTrackingListener(RedisListenerBase):
    """
    Listener for Redis client-side tracking invalidations with change detection.
    
    Uses RESP3 protocol with invalidation push handlers (single connection).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cache to track previous values: {key: value_hash}
        self._value_cache: Dict[str, str] = {}
        self._invalidation_queue: Optional[asyncio.Queue] = None

    def _compute_value_hash(self, value: Any) -> str:
        """Compute a hash of the value for change detection."""
        if isinstance(value, dict):
            value_str = str(sorted(value.items()))
        elif isinstance(value, (list, tuple)):
            value_str = str(value)
        else:
            value_str = str(value)
        
        return hashlib.sha256(value_str.encode()).hexdigest()

    def _has_value_changed(self, key: str, value: Any) -> bool:
        """Check if the value has changed since last seen."""
        value_hash = self._compute_value_hash(value)
        previous_hash = self._value_cache.get(key)
        
        if previous_hash == value_hash:
            return False
        
        self._value_cache[key] = value_hash
        return True

    async def _listen(self) -> None:
        """
        Listen for Redis client-side tracking invalidations via push handler.
        
        This method monitors the invalidation queue populated by the push handler.
        """
        logger.info("Starting Redis client-side tracking listener (RESP3)")

        # Get the invalidation queue from connection manager
        self._invalidation_queue = self.connection_manager.get_invalidation_queue()
        
        if not self._invalidation_queue:
            raise RuntimeError("No invalidation queue available - tracking not enabled")

        logger.info("Listening for invalidations via RESP3 push handler (queue ready, waiting for invalidations)...")

        try:
            ping_counter = 0
            # Get ping interval from config (default 0.5 seconds)
            ping_interval = self.connection_manager.config.redis.tracking_ping_interval_seconds
            logger.info(f"Using CLIENT TRACKING ping interval: {ping_interval}s")
            
            # Continuously process invalidations from the queue
            while self._running:
                try:
                    # Wait for invalidation with timeout to allow checking _running
                    invalidation_data = await asyncio.wait_for(
                        self._invalidation_queue.get(),
                        timeout=ping_interval
                    )
                    
                    logger.debug(f"[LISTENER] Got invalidation from queue (queue size now: {self._invalidation_queue.qsize()})")
                    await self._handle_invalidation(invalidation_data)
                    
                except asyncio.TimeoutError:
                    # No invalidation received, continue loop
                    # This allows us to check _running flag periodically
                    ping_counter += 1
                    # Log every 5 minutes (calculate iterations based on ping interval)
                    log_interval = int(300 / ping_interval)  # 300 seconds / ping_interval
                    if ping_counter % log_interval == 0:
                        logger.debug(f"[LISTENER] Still waiting for invalidations (queue size: {self._invalidation_queue.qsize()})")
                
                # Periodic ping to keep connection alive and trigger processing of pending invalidations
                # This is CRITICAL: Redis uses lazy processing, so we need regular commands to trigger invalidation delivery
                if self.connection_manager.redis_client:
                    try:
                        await self.connection_manager.redis_client.ping()
                    except Exception as e:
                        logger.warning(f"Ping failed: {e}")

        except asyncio.CancelledError:
            logger.info("Client-side tracking listener cancelled")
            raise
        finally:
            logger.info("Client-side tracking listener stopped")

    async def _handle_invalidation(self, invalidation_data: Any) -> None:
        """
        Handle an invalidation push message.
        
        Format: [b'invalidate', [b'key1', b'key2', ...]] or [b'invalidate', None]
        """
        try:
            if not invalidation_data or len(invalidation_data) < 2:
                logger.warning(f"Unexpected invalidation data format: {invalidation_data}")
                return
            
            command = invalidation_data[0]
            keys_data = invalidation_data[1]
            
            # Decode command if bytes
            if isinstance(command, bytes):
                command = command.decode('utf-8')
            elif not isinstance(command, str):
                command = str(command)
            
            if command != 'invalidate':
                logger.warning(f"Unexpected invalidation command: {command}")
                return
            
            # Handle flush event (None means all keys invalidated)
            if keys_data is None:
                logger.warning("Received FLUSH invalidation (all keys in slot invalidated)")
                # Optionally: clear value cache
                # self._value_cache.clear()
                return
            
            # Convert keys to list if needed
            if not isinstance(keys_data, list):
                keys_data = [keys_data]
            
            # Decode keys (handle both bytes and strings)
            invalidated_keys = []
            for key in keys_data:
                if isinstance(key, bytes):
                    try:
                        key = key.decode('utf-8')
                    except UnicodeDecodeError:
                        # If UTF-8 decode fails, skip this key
                        logger.warning(f"Cannot decode key as UTF-8, skipping: {key}")
                        continue
                elif not isinstance(key, str):
                    key = str(key)
                invalidated_keys.append(key)
            
            logger.debug(f"[LISTENER] Received invalidation for {len(invalidated_keys)} key(s): {invalidated_keys}")
            
            # Process each invalidated key
            for key in invalidated_keys:
                if key:
                    await self._process_invalidated_key(key)

        except Exception as e:
            logger.error(f"Error handling invalidation message: {e}", exc_info=True)

    async def _process_invalidated_key(self, key: str) -> None:
        """Process a single invalidated key."""
        try:
            logger.debug(f"[PROCESS] Processing invalidated key: {key}")
            
            # Check routing rule (apply fine-grained pattern matching)
            rule = self.routing_manager.find_matching_key_rule(key)
            if not rule:
                logger.debug(f"[PROCESS] Key {key} does not match any routing rule pattern - SKIPPING")
                return
            
            logger.debug(f"[PROCESS] Key {key} matches rule '{rule.name}' (pattern='{rule.redis_pattern}') -> table={rule.target_table}")
            
            # Fetch and process the key value and TTL
            result = await self._fetch_key_value(key, rule)
            if result is None:
                logger.warning(f"[PROCESS] Key {key} not found or empty in Redis")
                return
            
            value, ttl = result
            
            # Check if value actually changed
            if not self._has_value_changed(key, value):
                logger.debug(f"[PROCESS] Ignoring invalidation for {key}: value unchanged (hash match)")
                return
            
            logger.debug(f"[PROCESS] Value changed for key {key} (TTL={ttl}) - queueing event")
            
            # Queue the event with TTL
            await self._queue_event("key", key, value, rule, ttl=ttl)

        except Exception as e:
            logger.error(f"Error processing invalidated key {key}: {e}", exc_info=True)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type((redis.ConnectionError, redis.TimeoutError))
    )
    async def _fetch_key_value(self, key: str, rule: RoutingRule) -> tuple[Any, int | None] | None:
        """Fetch the value and TTL of a Redis key."""
        client = self.connection_manager.redis_client
        if not client:
            logger.warning(f"[FETCH] No Redis client available for key {key}")
            return None

        try:
            logger.debug(f"[FETCH] Fetching key {key} from Redis...")
            
            # Try string first, then hash
            value = await client.get(key)
            if value is None:
                logger.debug(f"[FETCH] Key {key} not a string, trying hash...")
                value = await client.hgetall(key)
                if not value:
                    logger.warning(f"[FETCH] Key {key} not found or empty (may have been deleted)")
                    return None
                logger.debug(f"[FETCH] Key {key} is a hash with {len(value)} fields")
            else:
                logger.debug(f"[FETCH] Key {key} is a string, length={len(value) if isinstance(value, (str, bytes)) else 'unknown'}")

            # Decode the value (handles both strings and msgpack)
            if isinstance(value, bytes):
                value = self.decoder.decode_binary_data(value)
            elif isinstance(value, dict):
                value = self.decoder.decode_hash_value(value)
            
            # Parse JSON if configured
            try:
                value = self.decoder.parse_json_if_needed(value, rule.value_is_object)
            except Exception as e:
                logger.debug(f"Error parsing value for key {key}: {e}")
                pass

            # Fetch TTL
            ttl_raw = await client.ttl(key)
            ttl = None if ttl_raw == -1 else ttl_raw
            logger.debug(f"[FETCH] Successfully fetched key {key}, TTL={ttl}")
            
            return (value, ttl)

        except Exception as e:
            logger.error(f"Error fetching key {key}: {e}")
            raise


class KeyspaceListener(RedisListenerBase):
    """Listener for Redis keyspace notifications with change detection."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Cache to track previous values: {key: value_hash}
        self._value_cache: Dict[str, str] = {}

    def _compute_value_hash(self, value: Any) -> str:
        """Compute a hash of the value for change detection."""
        # Convert value to a stable string representation
        if isinstance(value, dict):
            # Sort dict keys for consistent hashing
            value_str = str(sorted(value.items()))
        elif isinstance(value, (list, tuple)):
            value_str = str(value)
        else:
            value_str = str(value)
        
        # Return hash of the string representation
        return hashlib.sha256(value_str.encode()).hexdigest()

    def _has_value_changed(self, key: str, value: Any) -> bool:
        """Check if the value has changed since last seen.
        
        Returns:
            True if value changed or is new, False if unchanged
        """
        value_hash = self._compute_value_hash(value)
        previous_hash = self._value_cache.get(key)
        
        if previous_hash == value_hash:
            # Value hasn't changed
            return False
        
        # Update cache with new hash
        self._value_cache[key] = value_hash
        return True

    async def _listen(self) -> None:
        """Listen for Redis keyspace notifications."""
        logger.info("Starting Redis keyspace listener")

        client = self.connection_manager.redis_client
        if not client:
            raise redis.ConnectionError("No Redis client available")

        self.pubsub = client.pubsub()

        try:
            # Subscribe to keyevent notifications for 'set' operations
            # __keyevent@0__:set fires when any key is SET in database 0
            # The key name is sent in the message data
            await self.pubsub.psubscribe("__keyevent@*__:set")
            logger.info("Subscribed to keyevent pattern: __keyevent@*__:set")

            async for message in self.pubsub.listen():
                if not self._running:
                    break

                if message["type"] == "pmessage":
                    await self._handle_message(message)

        finally:
            if self.pubsub:
                await self.pubsub.close()
                self.pubsub = None

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle a Redis keyspace notification message."""
        try:
            # Keyevent notifications format:
            # channel: __keyevent@<db>__:<event> (e.g., "__keyevent@0__:set")
            # data: <key> (the key that was affected)
            channel = self._decode_if_bytes(message.get("channel"))
            key = self._decode_if_bytes(message.get("data"))
            
            if not key:
                return
            
            logger.debug(f"Keyspace event: key={key}, channel={channel}")

            # Check routing rule
            rule = self.routing_manager.find_matching_key_rule(key)
            if not rule:
                return

            # Fetch and process the key value and TTL
            result = await self._fetch_key_value(key, rule)
            if result is None:
                return
            
            value, ttl = result

            # Check if value actually changed
            if not self._has_value_changed(key, value):
                logger.debug(f"Ignoring keyspace event for {key}: value unchanged")
                return

            logger.debug(f"Value changed for key {key}, TTL={ttl}, queueing event")

            # Queue the event with TTL
            await self._queue_event("key", key, value, rule, ttl=ttl)

        except Exception as e:
            logger.error(f"Error handling keyspace message: {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(
            (redis.ConnectionError, redis.TimeoutError))
    )
    async def _fetch_key_value(self, key: str, rule: RoutingRule) -> tuple[Any, int | None] | None:
        """Fetch the value and TTL of a Redis key.
        
        Returns:
            Tuple of (value, ttl_seconds) or None if key not found
            ttl_seconds: None means no expiry, positive int is TTL in seconds
        """
        client = self.connection_manager.redis_client
        if not client:
            return None

        try:
            # Try string first, then hash
            value = await client.get(key)
            if value is None:
                value = await client.hgetall(key)
                if not value:
                    logger.warning(f"Key {key} not found or empty")
                    return None

            # Decode the value
            if isinstance(value, bytes):
                value = self.decoder.decode_binary_data(value)
            elif isinstance(value, dict):
                value = self.decoder.decode_hash_value(value)

            # Parse JSON if configured
            try:
                value = self.decoder.parse_json_if_needed(
                    value, rule.value_is_object)
            except:
                logger.error(f"Value of key {key} is not an object: {value}")
                pass

            # Fetch TTL for the key
            # -1 = no expiry (we'll convert to None), -2 = key doesn't exist, positive int = remaining TTL in seconds
            ttl_raw = await client.ttl(key)
            
            # Convert -1 (no expiry) to None for cleaner data representation
            # Keep positive values as-is (actual TTL in seconds)
            # -2 would indicate key doesn't exist, but we already verified the key exists
            ttl = None if ttl_raw == -1 else ttl_raw
            
            return (value, ttl)

        except Exception as e:
            logger.error(f"Error fetching key {key}: {e}")
            raise


class ChannelListener(RedisListenerBase):
    """Listener for Redis channel messages."""

    async def _listen(self) -> None:
        """Listen for channel messages."""
        logger.info("Starting channel subscriber")

        client = self.connection_manager.redis_client
        if not client:
            raise redis.ConnectionError("No Redis client available")

        self.pubsub = client.pubsub()

        try:
            # Subscribe to all channel patterns
            for pattern in self.routing_manager.get_channel_patterns():
                await self.pubsub.psubscribe(pattern)
                logger.info(f"Subscribed to channel pattern: {pattern}")

            async for message in self.pubsub.listen():
                if not self._running:
                    break

                if message["type"] == "pmessage":
                    await self._handle_message(message)

        finally:
            if self.pubsub:
                await self.pubsub.close()
                self.pubsub = None

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle a channel message."""
        try:
            channel = self._decode_if_bytes(message.get("channel"))
            data = message.get("data")

            logger.debug(channel, data)

            # Decode data
            if isinstance(data, bytes):
                data = self.decoder.decode_binary_data(data)

            # logger.info(f"Received channel message: channel={channel}")

            # Find matching rule
            rule = self.routing_manager.find_matching_channel_rule(channel)
            if not rule:
                return

            # Parse JSON if configured
            try:
                data = self.decoder.parse_json_if_needed(
                    data, rule.value_is_object)
            except:
                logger.error(f"Data of key {channel} is not an object: {data}")
                pass

            # Queue the event
            await self._queue_event("channel", channel, data, rule)

        except Exception as e:
            logger.error(f"Error handling channel message: {e}", exc_info=True)
