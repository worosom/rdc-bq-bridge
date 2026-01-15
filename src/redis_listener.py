"""Redis listener implementations for keyspace and channel subscriptions."""

import hashlib
import logging
from typing import Any, Dict

import redis.asyncio as redis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import RoutingRule
from .listener_base import RedisListenerBase

logger = logging.getLogger(__name__)


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
