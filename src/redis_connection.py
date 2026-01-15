"""Redis connection management with retry logic."""

import asyncio
import logging
from typing import Optional

import redis.asyncio as redis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import Config

logger = logging.getLogger(__name__)


class RedisConnectionManager:
    """Manages Redis connections with automatic reconnection and health checks."""

    def __init__(self, config: Config):
        self.config = config
        self.redis_client: Optional[redis.Redis] = None
        self._reconnect_delay = 1.0  # Initial reconnect delay in seconds
        self._max_reconnect_delay = 60.0  # Maximum reconnect delay

    async def create_connection(self) -> redis.Redis:
        """Create a new Redis connection with retry logic."""
        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            try:
                client = redis.Redis(
                    host=self.config.redis.host,
                    port=self.config.redis.port,
                    username=self.config.redis.username,
                    password=self.config.redis.password,
                    socket_keepalive=True,
                    health_check_interval=30  # Health check every 30 seconds
                )

                # Test the connection
                await client.ping()
                logger.info("Redis connection established successfully")
                self._reconnect_delay = 1.0  # Reset delay on successful connection
                self.redis_client = client
                return client

            except (redis.ConnectionError, redis.TimeoutError) as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(
                        f"Failed to connect to Redis after {max_retries} attempts")
                    raise

                # Exponential backoff with max 30 seconds
                wait_time = min(2 ** retry_count, 30)
                logger.warning(
                    f"Redis connection failed (attempt {retry_count}/{max_retries}), retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)

        raise redis.ConnectionError("Could not establish Redis connection")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((redis.ConnectionError, redis.TimeoutError))
    )
    async def test_connection(self) -> None:
        """Test Redis connection."""
        if not self.redis_client:
            raise redis.ConnectionError("No Redis client available")
        await self.redis_client.ping()
        logger.info("Redis connection test successful")

    async def close(self) -> None:
        """Close the Redis connection."""
        if self.redis_client:
            try:
                await self.redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.warning(f"Error closing Redis client: {e}")
            self.redis_client = None

    def get_reconnect_delay(self, consecutive_errors: int) -> float:
        """Calculate reconnect delay based on consecutive errors."""
        return min(
            self._reconnect_delay * (2 ** (consecutive_errors - 1)),
            self._max_reconnect_delay
        )