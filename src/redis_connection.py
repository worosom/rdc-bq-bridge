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
        self._invalidation_queue: Optional[asyncio.Queue] = None  # Queue for client-side tracking invalidations

    async def create_connection(self) -> redis.Redis:
        """Create a new Redis connection with client-side tracking enabled (RESP3).
        
        If a connection already exists, returns it without creating a new one.
        """
        # Return existing connection if available
        if self.redis_client is not None:
            logger.info("Redis connection already exists, reusing existing connection")
            return self.redis_client
        
        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            try:
                client = await self._create_tracking_connection()

                # Test the connection
                await client.ping()
                logger.info("Redis connection established successfully with client-side tracking (RESP3)")
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
    
    async def _create_tracking_connection(self) -> redis.Redis:
        """Create Redis connection with client-side tracking enabled (RESP3)."""
        from .routing_manager import RoutingManager
        
        logger.info("Creating Redis connection with CLIENT TRACKING (RESP3)...")
        
        # Create invalidation queue
        self._invalidation_queue = asyncio.Queue()
        logger.info("Invalidation queue created")
        
        # Create client with RESP3 protocol
        client = redis.Redis(
            host=self.config.redis.host,
            port=self.config.redis.port,
            username=self.config.redis.username,
            password=self.config.redis.password,
            socket_keepalive=True,
            health_check_interval=30,
            decode_responses=False,  # Keep binary - we have msgpack data
            protocol=3,  # RESP3 protocol for push support
            single_connection_client=True  # Important for consistent tracking
        )
        
        # Extract prefixes from routing rules
        routing_manager = RoutingManager(self.config)
        prefixes = routing_manager.get_key_prefixes()
        
        logger.info(f"Enabling CLIENT TRACKING BCAST with {len(prefixes)} prefixes: {prefixes}")
        
        # Enable client-side tracking with BCAST mode
        result = await client.client_tracking(
            on=True,
            bcast=True,
            prefix=prefixes,
            noloop=True  # Don't send invalidations for keys modified by this connection
        )
        logger.info(f"CLIENT TRACKING enabled successfully: {result}")
        
        # Register invalidation push handler
        invalidation_count = [0]  # Use list to allow mutation in nested function
        
        async def invalidation_handler(invalidation_data):
            """
            Handle invalidation push messages.
            
            invalidation_data format:
                [b'invalidate', [b'key1', b'key2', ...]]  # List of keys
                [b'invalidate', None]                     # Flush event
            """
            try:
                invalidation_count[0] += 1
                logger.debug(f"[HANDLER] Invalidation #{invalidation_count[0]} received, adding to queue")
                # Put invalidation in queue asynchronously
                await self._invalidation_queue.put(invalidation_data)
                logger.debug(f"[HANDLER] Invalidation #{invalidation_count[0]} added to queue (queue size: {self._invalidation_queue.qsize()})")
            except Exception as e:
                logger.error(f"Error in invalidation handler: {e}")
        
        # Set the handler on the connection parser
        client.connection._parser.set_invalidation_push_handler(invalidation_handler)
        
        logger.info("Invalidation push handler registered successfully")
        
        return client
    
    def get_invalidation_queue(self) -> Optional[asyncio.Queue]:
        """Get the invalidation queue for processing (client-side tracking mode)."""
        return self._invalidation_queue

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((redis.ConnectionError, redis.TimeoutError))
    )
    async def test_connection(self) -> None:
        """Test Redis connection."""
        if not self.redis_client:
            raise redis.ConnectionError("No Redis client available")
        await client.ping()
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
            self._invalidation_queue = None

    def get_reconnect_delay(self, consecutive_errors: int) -> float:
        """Calculate reconnect delay based on consecutive errors."""
        return min(
            self._reconnect_delay * (2 ** (consecutive_errors - 1)),
            self._max_reconnect_delay
        )