"""Base class for Redis listeners with common functionality."""

import asyncio
import logging
from typing import Any, Optional

import redis.asyncio as redis
from redis.asyncio.client import PubSub

from .config import RoutingRule
from .redis_data_event import RedisDataEvent
from .redis_decoder import RedisDataDecoder

logger = logging.getLogger(__name__)


class RedisListenerBase:
    """Base class for Redis listeners with common functionality."""

    def __init__(self, connection_manager, routing_manager, output_queue):
        self.connection_manager = connection_manager
        self.routing_manager = routing_manager
        self.output_queue = output_queue
        self.decoder = RedisDataDecoder()
        self.pubsub: Optional[PubSub] = None
        self._running = False

    async def start(self) -> None:
        """Start the listener."""
        self._running = True
        await self._listener_with_reconnect()

    async def stop(self) -> None:
        """Stop the listener."""
        self._running = False
        if self.pubsub:
            try:
                await self.pubsub.close()
            except Exception as e:
                logger.warning(f"Error closing pubsub connection: {e}")
            self.pubsub = None

    async def _listener_with_reconnect(self) -> None:
        """Listener with reconnection logic."""
        consecutive_errors = 0
        max_consecutive_errors = 10
        listener_name = self.__class__.__name__

        while self._running:
            try:
                await self._listen()
                consecutive_errors = 0

            except (redis.ConnectionError, redis.TimeoutError, ConnectionError) as e:
                consecutive_errors += 1
                logger.error(
                    f"Redis connection error in {listener_name} "
                    f"(error {consecutive_errors}/{max_consecutive_errors}): {e}"
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors, stopping {listener_name}")
                    raise

                delay = self.connection_manager.get_reconnect_delay(consecutive_errors)
                logger.info(f"Attempting to reconnect {listener_name} in {delay:.1f} seconds...")
                await asyncio.sleep(delay)

                # Try to recreate the connection
                try:
                    await self.connection_manager.create_connection()
                    logger.info(f"Successfully reconnected to Redis for {listener_name}")
                except Exception as reconnect_error:
                    logger.error(f"Failed to reconnect to Redis: {reconnect_error}")

            except asyncio.CancelledError:
                logger.info(f"{listener_name} cancelled")
                break

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Unexpected error in {listener_name}: {e}", exc_info=True)
                if consecutive_errors >= max_consecutive_errors:
                    raise
                await asyncio.sleep(5)

    async def _listen(self) -> None:
        """Override in subclasses to implement specific listening logic."""
        raise NotImplementedError

    async def _queue_event(self, source_type: str, key_or_channel: str,
                          value: Any, rule: RoutingRule, ttl: Optional[int] = None) -> None:
        """Queue a Redis data event.
        
        Args:
            source_type: "key" or "channel"
            key_or_channel: The Redis key or channel name
            value: The value/payload
            rule: Routing rule that matched
            ttl: TTL in seconds (None = no expiry, positive = TTL)
        """
        event = RedisDataEvent(
            source_type=source_type,
            key_or_channel=key_or_channel,
            value=value,
            routing_rule=rule,
            ttl=ttl
        )
        await self.output_queue.put(event)
        ttl_str = f", TTL={ttl}s" if ttl is not None else ""
        logger.debug(f"Queued {source_type} event: {key_or_channel} -> {rule.target_table}{ttl_str}")

    def _decode_if_bytes(self, value: Any) -> str:
        """Decode bytes to string if needed."""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value