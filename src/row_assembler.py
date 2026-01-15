"""Stream processing logic for the RDC-BigQuery bridge."""

import asyncio
import logging
from typing import Any, Dict

from src.config import Config
from .redis_ingestor import RedisDataEvent
from .row_models import AssembledRow, RowInProgress
from .row_processor import RowProcessor

logger = logging.getLogger(__name__)


class RowAssembler:
    """
    Handles immediate processing of rows from Redis events.
    Formerly 'RowAssembler', now acting more as a StreamProcessor.
    """

    def __init__(self, config: Config,
                 input_queue: asyncio.Queue[RedisDataEvent],
                 output_queue: asyncio.Queue[AssembledRow]):
        self.config = config
        self.input_queue = input_queue
        self.output_queue = output_queue

        self.row_processor = RowProcessor(config=config)
        self.rows_processed = 0
        self._running = False

    async def start(self) -> None:
        """Start the processor."""
        logger.info("Starting stream processor...")
        self._running = True
        try:
            await self._process_events()
        except asyncio.CancelledError:
            logger.info("Stream processor cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in stream processor: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the processor."""
        self._running = False
        logger.info(f"Stream processor stopped. Processed: {self.rows_processed}")

    async def _process_events(self) -> None:
        """Main loop."""
        while self._running:
            try:
                event = await self.input_queue.get()
                await self._process_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing event: {e}", exc_info=True)

    async def _process_event(self, event: RedisDataEvent) -> None:
        """Process a single event and immediately output a row."""
        
        if not event.routing_rule:
            return

        target_table = event.routing_rule.target_table
        
        # Extract initial ID
        row_id = self.row_processor.extract_row_id(event) or event.key_or_channel
        
        # Create row object
        row = RowInProgress(row_id=row_id, target_table=target_table)
        
        # Extract data
        data_type = self.row_processor.extract_data_type(event)
        await self.row_processor.add_data_to_row(row, event, data_type)
        
        # If no data was extracted, skip
        if not row.data:
            return

        # Create assembled row
        assembled_row = AssembledRow(
            target_table=target_table,
            data=row.data
        )
        
        # Send to loader
        await self.output_queue.put(assembled_row)
        self.rows_processed += 1


    def get_stats(self) -> Dict[str, Any]:
        return {
            "rows_processed": self.rows_processed
        }