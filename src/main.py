"""Main application entry point for the RDC-BigQuery bridge."""

import argparse
import asyncio
import logging
import signal
import sys
from typing import Optional

from .bq_loader import BigQueryLoaderManager
from .bq_schema_utils import initialize_schemas
from .bq_setup import BigQuerySetup, setup_bigquery_infrastructure, verify_bigquery_infrastructure
from .config import Config, ConfigValidationError, get_default_config_path, load_config
from .device_ticket_mapper import DeviceTicketMapper
from .redis_ingestor import RedisDataEvent, RedisIngestor
from .row_assembler import AssembledRow, RowAssembler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('rdc_bridge.log')
    ]
)

# Suppress noisy internal redis-py logs
logging.getLogger('push_response').setLevel(logging.WARNING)
logging.getLogger('redis').setLevel(logging.WARNING)
logging.getLogger('redis.asyncio').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class RDCBigQueryBridge:
    """Main application class for the RDC-BigQuery bridge."""

    def __init__(self, config: Config, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run

        # Create queues for inter-component communication
        self.redis_to_assembler_queue: asyncio.Queue[RedisDataEvent] = asyncio.Queue(maxsize=10000)
        self.assembler_to_loader_queue: asyncio.Queue[AssembledRow] = asyncio.Queue(maxsize=5000)

        # Initialize device-to-ticket mapper
        self.device_mapper = DeviceTicketMapper()

        # Initialize components
        self.redis_ingestor = RedisIngestor(config, self.redis_to_assembler_queue)
        self.row_assembler = RowAssembler(
            config, 
            self.redis_to_assembler_queue, 
            self.assembler_to_loader_queue,
            device_mapper=self.device_mapper
        )
        self.bq_loader_manager = BigQueryLoaderManager(config, self.assembler_to_loader_queue, dry_run=dry_run)

        # Task group for managing all components
        self.task_group: Optional[asyncio.TaskGroup] = None
        self._shutdown_event = asyncio.Event()

        if dry_run:
            logger.info("Initialized RDC-BigQuery Bridge in DRY-RUN mode (no data will be written to BigQuery)")
        else:
            logger.info("Initialized RDC-BigQuery Bridge")

    async def start(self) -> None:
        """Start all components of the bridge."""
        logger.info("Starting RDC-BigQuery Bridge...")

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

        # Bootstrap device-to-ticket mappings from Redis
        logger.info("Bootstrapping device-to-ticket mappings from Redis...")
        try:
            # Get Redis connection for bootstrapping
            await self.redis_ingestor.connection_manager.create_connection()
            redis_client = self.redis_ingestor.connection_manager.redis_client
            
            if redis_client:
                count = await self.device_mapper.bootstrap_from_redis(redis_client)
                logger.info(f"Bootstrap complete: {count} device-ticket mappings loaded")
            else:
                logger.warning("Could not get Redis connection for bootstrap")
        except Exception as e:
            logger.error(f"Error during device mapper bootstrap: {e}")
            logger.warning("Continuing without bootstrap - mappings will be populated as events arrive")

        # Create tasks for all components
        tasks = []

        try:
            # Start Redis ingestion
            redis_task = asyncio.create_task(
                self.redis_ingestor.start(),
                name="redis_ingestor"
            )
            tasks.append(redis_task)

            # Start row assembly
            assembler_task = asyncio.create_task(
                self.row_assembler.start(),
                name="row_assembler"
            )
            tasks.append(assembler_task)

            # Start BigQuery loaders
            loader_task = asyncio.create_task(
                self.bq_loader_manager.start(),
                name="bq_loader_manager"
            )
            tasks.append(loader_task)

            # Start monitoring task
            monitor_task = asyncio.create_task(
                self._monitor_components(),
                name="monitor"
            )
            tasks.append(monitor_task)

            logger.info("All components started successfully")

            # Wait for shutdown signal or any task to fail (not just complete)
            # We use a loop to continuously check task status
            while not self._shutdown_event.is_set():
                # Check if any tasks have failed
                for task in tasks:
                    if task.done():
                        # Task completed - check if it was due to an error
                        try:
                            # This will raise if the task had an exception
                            task.result()
                            # If we get here, the task completed normally (which shouldn't happen)
                            logger.warning(f"Task {task.get_name()} completed unexpectedly without error")
                            # Don't shut down for normal completion - these tasks should run forever
                            # Only shut down if it's a critical component
                            if task.get_name() in ["redis_ingestor", "row_assembler", "bq_loader_manager"]:
                                logger.error(f"Critical task {task.get_name()} stopped unexpectedly, initiating shutdown")
                                self._shutdown_event.set()
                                break
                        except asyncio.CancelledError:
                            # Task was cancelled - this is expected during shutdown
                            logger.info(f"Task {task.get_name()} was cancelled")
                        except Exception as e:
                            # Task failed with an error
                            logger.error(f"Task {task.get_name()} failed with error: {e}")
                            self._shutdown_event.set()
                            break

                # Wait a bit before checking again
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                    # If we get here, shutdown was requested
                    break
                except asyncio.TimeoutError:
                    # Timeout is expected - continue checking
                    pass

            logger.info("Shutdown initiated, cancelling remaining tasks...")

            # Cancel all pending tasks
            for task in tasks:
                if not task.done():
                    task.cancel()

            # Wait for tasks to finish cancellation
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error during startup: {e}")
            # Cancel all tasks if startup fails
            for task in tasks:
                if not task.done():
                    task.cancel()
            raise

        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop all components gracefully."""
        logger.info("Stopping RDC-BigQuery Bridge...")

        try:
            # Stop components in reverse order
            await self.bq_loader_manager.stop()
            await self.row_assembler.stop()
            await self.redis_ingestor.stop()

            logger.info("All components stopped successfully")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            # Set the shutdown event - this will be checked by the main loop
            self._shutdown_event.set()

        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, signal_handler)

    async def _initiate_shutdown(self) -> None:
        """Initiate graceful shutdown."""
        logger.info("Initiating graceful shutdown...")
        self._shutdown_event.set()

    async def _monitor_components(self) -> None:
        """Monitor component health and log statistics."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(30)  # Monitor every 30 seconds

                # Log queue sizes
                redis_queue_size = self.redis_to_assembler_queue.qsize()
                assembler_queue_size = self.assembler_to_loader_queue.qsize()

                logger.info(
                    f"Queue sizes - Redis->Assembler: {redis_queue_size}, "
                    f"Assembler->Loader: {assembler_queue_size}"
                )

                # Log component statistics
                assembler_stats = self.row_assembler.get_stats()
                loader_stats = self.bq_loader_manager.get_stats()
                mapper_stats = self.device_mapper.get_stats()

                logger.info(f"Row assembler stats: {assembler_stats}")
                logger.info(f"BigQuery loader stats: {loader_stats}")
                logger.info(f"Device mapper stats: {mapper_stats}")

                # Check for potential issues
                if redis_queue_size > 8000:
                    logger.warning("Redis->Assembler queue is getting full!")

                if assembler_queue_size > 4000:
                    logger.warning("Assembler->Loader queue is getting full!")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring: {e}")


async def main(dry_run: bool = False) -> None:
    """Main entry point for the application."""
    try:
        # Load configuration
        config_path = get_default_config_path()
        logger.info(f"Loading configuration from: {config_path}")

        config = load_config(config_path)
        logger.info("Configuration loaded and validated successfully")

        # Initialize schema utilities with config
        initialize_schemas(config)
        logger.info("Schema utilities initialized with configuration")

        # Setup BigQuery infrastructure (dataset and tables)
        logger.info("Setting up BigQuery infrastructure...")
        try:
            await setup_bigquery_infrastructure(config)
            logger.info("BigQuery infrastructure setup completed")

            # Verify infrastructure is properly configured with retry
            logger.info("Verifying BigQuery infrastructure readiness...")
            max_retries = 3
            verification_passed = False
            
            for attempt in range(max_retries):
                if await verify_bigquery_infrastructure(config):
                    logger.info("BigQuery infrastructure verification passed")
                    verification_passed = True
                    break
                else:
                    if attempt < max_retries - 1:
                        wait_time = 3 * (attempt + 1)  # 3, 6, 9 seconds
                        logger.warning(f"BigQuery infrastructure verification failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.warning("BigQuery infrastructure verification failed after all retries, but continuing...")

            # Verify Storage Write API readiness
            if verification_passed:
                logger.info("Verifying Storage Write API readiness...")
                bq_setup = BigQuerySetup(config)
                await bq_setup.initialize()
                
                max_write_api_retries = 5
                write_api_ready = False
                
                for attempt in range(max_write_api_retries):
                    if await bq_setup.verify_storage_write_api_ready():
                        logger.info("Storage Write API verification passed")
                        write_api_ready = True
                        break
                    else:
                        if attempt < max_write_api_retries - 1:
                            wait_time = 2 ** (attempt + 1)  # 2, 4, 8, 16, 32 seconds exponential backoff
                            logger.warning(f"Storage Write API not ready (attempt {attempt + 1}/{max_write_api_retries}), waiting {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error("Storage Write API verification failed after all retries. Tables may not be ready for write streams.")
                            logger.error("Consider waiting a few minutes and restarting the application.")
                
                if not write_api_ready:
                    logger.error("Failed setting up Storage Write API for all tables")
                    exit(1)
                    # logger.warning("Proceeding despite Storage Write API verification failure - expect potential errors on startup")

        except Exception as e:
            logger.error(f"Failed to setup BigQuery infrastructure: {e}")
            logger.error("This may cause issues during data loading. Please check your BigQuery setup.")
            # Continue anyway - tables might already exist or be created manually

        # Create and start the bridge
        bridge = RDCBigQueryBridge(config, dry_run=dry_run)
        await bridge.start()

    except ConfigValidationError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


def cli_main() -> None:
    """CLI entry point (for use with poetry scripts)."""
    parser = argparse.ArgumentParser(
        description='RDC-BigQuery Bridge: Real-time data synchronization from Redis to BigQuery'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (process data but do not write to BigQuery)'
    )
    
    args = parser.parse_args()
    
    try:
        asyncio.run(main(dry_run=args.dry_run))
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    cli_main()