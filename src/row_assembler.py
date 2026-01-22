"""Stream processing logic for the RDC-BigQuery bridge."""

import asyncio
import logging
import msgpack
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
                 output_queue: asyncio.Queue[AssembledRow],
                 device_mapper=None):
        self.config = config
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.device_mapper = device_mapper

        self.row_processor = RowProcessor(
            config=config,
            device_mapper=device_mapper
        )
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
        
        # Handle device assignment events to update the mapper
        if self.device_mapper and event.key_or_channel.endswith(":EmpaticaDeviceID"):
            await self._handle_device_assignment(event)
        
        # Handle visitor status changes to release mappings when inactive
        if self.device_mapper and event.key_or_channel.endswith(":Status"):
            await self._handle_visitor_status(event)
        
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
            logger.warning(f"[ASSEMBLER] No data extracted from event for key {event.key_or_channel}, skipping")
            return

        logger.debug(f"[ASSEMBLER] Assembled row for table={target_table}, row_id={row_id}, fields={list(row.data.keys())}")

        # Create assembled row
        assembled_row = AssembledRow(
            target_table=target_table,
            data=row.data
        )
        
        # Send to loader
        await self.output_queue.put(assembled_row)
        self.rows_processed += 1
        logger.debug(f"[ASSEMBLER] Row sent to loader queue (queue_size={self.output_queue.qsize()}, total_processed={self.rows_processed})")
    
    async def _handle_device_assignment(self, event: RedisDataEvent) -> None:
        """
        Handle device-to-ticket assignment events.
        
        This updates the device mapper when a Visitors:*:EmpaticaDeviceID
        key is set, changed, or deleted.
        
        Scenarios handled:
        1. New assignment: Creates device→ticket mapping
        2. Device change: Updates to new device, releases old device
        3. Device release: Empty/null value releases the mapping
        
        Note: The device_id value is msgpack-encoded in Redis.
        """
        try:
            # Extract ticket_id from key: "Visitors:TICKET123:EmpaticaDeviceID"
            parts = event.key_or_channel.split(":")
            if len(parts) >= 3 and parts[0] == "Visitors":
                ticket_id = parts[1].strip()
                
                # Value is the device_id (msgpack-encoded)
                device_id_raw = event.value
                
                # Decode msgpack value
                device_id = None
                if device_id_raw is not None:
                    try:
                        if isinstance(device_id_raw, bytes):
                            device_id = msgpack.unpackb(device_id_raw, raw=False)
                        else:
                            device_id = device_id_raw
                    except (msgpack.exceptions.ExtraData, msgpack.exceptions.UnpackException) as e:
                        # Fallback: try as plain string
                        logger.debug(f"Failed to decode msgpack, trying plain decode: {e}")
                        if isinstance(device_id_raw, bytes):
                            try:
                                device_id = device_id_raw.decode('utf-8')
                            except UnicodeDecodeError:
                                logger.warning(
                                    f"Could not decode device_id for {event.key_or_channel}"
                                )
                                return
                        else:
                            device_id = str(device_id_raw)
                    
                    # Normalize device_id
                    device_id = str(device_id).strip() if device_id else None
                
                # Handle the assignment based on the device_id value
                if device_id:
                    # Case 1 & 2: New assignment or device change
                    # The update_mapping method already handles device reassignments
                    # and will log if the device changes for this ticket
                    await self.device_mapper.update_mapping(
                        device_id=device_id,
                        ticket_id=ticket_id
                    )
                    logger.info(
                        f"Device assignment updated: {device_id} → {ticket_id}"
                    )
                else:
                    # Case 3: Empty/null device_id means release the mapping
                    # Find the current device for this ticket and remove it
                    old_device_id = await self.device_mapper.get_device_for_ticket(ticket_id)
                    if old_device_id:
                        await self.device_mapper.remove_mapping(old_device_id)
                        logger.info(
                            f"Device mapping released (empty device_id): "
                            f"{old_device_id} ↔ {ticket_id}"
                        )
                    else:
                        logger.debug(
                            f"No existing device mapping to release for ticket: {ticket_id}"
                        )
        except Exception as e:
            logger.error(
                f"Error handling device assignment for {event.key_or_channel}: {e}",
                exc_info=True
            )
    
    async def _handle_visitor_status(self, event: RedisDataEvent) -> None:
        """
        Handle visitor status change events.
        
        When a visitor's status becomes "Inactive", release their device mapping
        so the device can be reassigned to another visitor.
        
        Expected key format: "Visitors:TICKET123:Status"
        Expected value for release: "Inactive"
        """
        try:
            # Extract ticket_id from key: "Visitors:TICKET123:Status"
            parts = event.key_or_channel.split(":")
            if len(parts) >= 3 and parts[0] == "Visitors" and parts[2] == "Status":
                ticket_id = parts[1]
                
                # Get the status value
                status_value = event.value
                
                # Decode if bytes
                if isinstance(status_value, bytes):
                    try:
                        # Try msgpack first
                        status_value = msgpack.unpackb(status_value, raw=False)
                    except (msgpack.exceptions.ExtraData, msgpack.exceptions.UnpackException):
                        # Fallback to plain string decode
                        try:
                            status_value = status_value.decode('utf-8')
                        except UnicodeDecodeError:
                            logger.warning(
                                f"Could not decode status value for {event.key_or_channel}"
                            )
                            return
                
                # Convert to string and normalize
                status_value = str(status_value).strip() if status_value else ""
                
                # Check if visitor is becoming inactive
                if status_value == "Inactive":
                    # Find the device_id associated with this ticket_id
                    device_id = await self.device_mapper.get_device_for_ticket(ticket_id)
                    
                    if device_id:
                        # Remove the mapping
                        await self.device_mapper.remove_mapping(device_id)
                        logger.info(
                            f"Released device mapping due to inactive status: "
                            f"{device_id} ↔ {ticket_id}"
                        )
                    else:
                        logger.debug(
                            f"No device mapping found for inactive visitor: {ticket_id}"
                        )
        except Exception as e:
            logger.error(
                f"Error handling visitor status for {event.key_or_channel}: {e}",
                exc_info=True
            )


    def get_stats(self) -> Dict[str, Any]:
        return {
            "rows_processed": self.rows_processed
        }