"""Redis data ingestion component for the RDC-BigQuery bridge."""

import asyncio
import logging

from .config import Config
from .redis_connection import RedisConnectionManager
from .redis_data_event import RedisDataEvent
from .redis_listener import ClientSideTrackingListener, ChannelListener
from .routing_manager import RoutingManager

logger = logging.getLogger(__name__)


class RedisIngestor:
    """Handles Redis data ingestion through client-side tracking invalidations and channel subscriptions."""

    def __init__(self, config: Config, output_queue: asyncio.Queue[RedisDataEvent]):
        self.config = config
        self.output_queue = output_queue
        self._running = False

        # Initialize managers
        self.connection_manager = RedisConnectionManager(config)
        self.routing_manager = RoutingManager(config)

        # Initialize listeners
        self.tracking_listener = None
        self.channel_listener = None

        if self.routing_manager.has_key_rules():
            self.tracking_listener = ClientSideTrackingListener(
                self.connection_manager,
                self.routing_manager,
                self.output_queue
            )
            logger.info("Using ClientSideTrackingListener for key events (RESP3)")

        if self.routing_manager.has_channel_rules():
            self.channel_listener = ChannelListener(
                self.connection_manager,
                self.routing_manager,
                self.output_queue
            )

    async def start(self) -> None:
        """Start the Redis ingestion process."""
        logger.info("Starting Redis ingestion with client-side tracking (RESP3)...")

        try:
            # Initialize Redis connection
            await self.connection_manager.create_connection()

            self._running = True

            # Start ingestion tasks
            tasks = []

            if self.tracking_listener:
                tasks.append(asyncio.create_task(
                    self.tracking_listener.start(),
                    name="tracking_listener"
                ))

            if self.channel_listener:
                tasks.append(asyncio.create_task(
                    self.channel_listener.start(),
                    name="channel_listener"
                ))

            if not tasks:
                logger.warning("No routing rules configured, Redis ingestion will not start")
                return

            # Wait for all tasks to complete
            await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            logger.info("Redis ingestion cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in Redis ingestion: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the Redis ingestion process."""
        logger.info("Stopping Redis ingestion...")
        self._running = False

        # Stop listeners
        if self.tracking_listener:
            await self.tracking_listener.stop()

        if self.channel_listener:
            await self.channel_listener.stop()

        # Close Redis connection
        await self.connection_manager.close()
