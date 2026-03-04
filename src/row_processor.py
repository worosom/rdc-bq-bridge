"""Event processing and data transformation for row assembly."""

import json
import logging
from datetime import datetime, timezone, date
from typing import Any, Dict, Optional

import msgpack
import pandas as pd

from .redis_ingestor import RedisDataEvent
from .row_models import RowInProgress

logger = logging.getLogger(__name__)


def _json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, msgpack.ext.Timestamp):
        # Convert msgpack Timestamp to datetime, then to ISO string
        return obj.to_datetime().isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


class RowProcessor:
    """Handles processing of Redis events and data transformation."""

    def __init__(self, config=None, aggregation_handler=None, device_mapper=None):
        """
        Initialize the row processor.
        Args:
            config: Configuration object containing field mappings.
            aggregation_handler: Ignored in simplified pipeline.
            device_mapper: DeviceTicketMapper for enriching data with ticket_id.
        """
        self.config = config
        self.device_mapper = device_mapper
        self._field_mappings_cache = {}
        if config and config.bigquery:
            # Cache field mappings for each table
            for table_name, table_config in config.bigquery.tables.items():
                self._field_mappings_cache[table_name] = table_config.field_mappings

    def extract_row_id(self, event: RedisDataEvent) -> Optional[str]:
        """Extract row ID from the event using the routing rule regex."""
        if not event.routing_rule:
            return None

        # For global state events, each key change creates its own row
        # Use the full key as the row ID
        if event.routing_rule.target_table == "global_state_events":
            return event.key_or_channel

        # For other events, use regex parsing
        if not event.routing_rule.compiled_regex:
            return None

        match = event.routing_rule.compiled_regex.match(event.key_or_channel)
        if not match:
            return None

        match_dict = match.groupdict()

        # Check if this is a ticket_id pattern
        if "ticket_id" in match_dict:
            return match_dict.get("ticket_id")
        # Otherwise use device_id
        elif "device_id" in match_dict:
            return match_dict.get("device_id")

        return None

    def extract_data_type(self, event: RedisDataEvent) -> str:
        """Extract the data type from the event."""
        key_or_channel = event.key_or_channel

        if "BioSensors" in key_or_channel:
            return "BioSensors"
        elif "BlueIoTSensors" in key_or_channel:
            return "BlueIoTSensors"
        elif "Status" in key_or_channel:
            return "Status"
        elif "EmpaticaDeviceID" in key_or_channel:
            return "EmpaticaDeviceID"
        else:
            parts = key_or_channel.split(":")
            return parts[-1] if parts else "Unknown"

    async def add_data_to_row(self, row: RowInProgress, event: RedisDataEvent,
                              data_type: str) -> None:
        """Add data from the event to the row."""
        try:
            # First, try to extract IDs from the key/routing rule
            # Skip this for global_state_events which doesn't have device_id/ticket_id fields
            if row.target_table != "global_state_events" and event.routing_rule and event.routing_rule.compiled_regex:
                match = event.routing_rule.compiled_regex.match(event.key_or_channel)
                if match:
                    match_dict = match.groupdict()
                    if "device_id" in match_dict:
                        extracted_device_id = match_dict["device_id"]
                        if extracted_device_id:
                            row.data["device_id"] = extracted_device_id
                            logger.debug(f"Extracted device_id from regex: {extracted_device_id}")
                        else:
                            # Regex matched but captured empty device_id - invalid data
                            raise ValueError(f"Regex matched but device_id is empty for key: {event.key_or_channel}")
                else:
                    # Regex failed to match - invalid channel name for this table
                    raise ValueError(
                        f"Regex pattern '{event.routing_rule.redis_pattern}' failed to match "
                        f"key/channel '{event.key_or_channel}' for table {row.target_table}"
                    )

            row.received_data_types.add(data_type)

            if row.target_table == "empatica":
                await self._add_biosensor_data(row, event)
            elif row.target_table == "blueiot":
                await self._add_blueiot_sensor_data(row, event)
            elif row.target_table == "global_state_events":
                await self._add_global_state_data(row, event)
            else:
                logger.warning(f"Unknown target table: {row.target_table}")

            row.last_update = datetime.now(timezone.utc)

        except Exception as e:
            logger.error(
                f"Error adding data to row {row.row_id} for table {row.target_table}, "
                f"key/channel: {event.key_or_channel}: {e}", 
                exc_info=True
            )

    async def _add_biosensor_data(self, row: RowInProgress,
                                  event: RedisDataEvent) -> None:
        """Process biosensor data from the event."""
        try:
            # Parse biosensor JSON data
            if isinstance(event.value, str):
                try:
                    data = json.loads(event.value)
                except json.JSONDecodeError:
                    return
            elif isinstance(event.value, dict):
                data = event.value
            else:
                return

            # Get field mappings for empatica table
            field_mappings = self._field_mappings_cache.get("empatica", {})
            
            # Apply field mappings parametrically
            for source_field, target_field in field_mappings.items():
                if source_field in data:
                    # For device_id: allow overwrite if it's None or empty (from failed regex extraction)
                    # For other fields: don't overwrite existing data
                    should_set = target_field not in row.data or (
                        target_field == "device_id" and not row.data.get("device_id")
                    )
                    
                    if should_set:
                        try:
                            # Convert to float for numeric fields
                            row.data[target_field] = float(data[source_field])
                        except (ValueError, TypeError):
                            # If conversion fails, store as-is
                            row.data[target_field] = data[source_field]
            
            # Enrich with ticket_id from device_id mapping
            if self.device_mapper and "device_id" in row.data:
                ticket_id = await self.device_mapper.get_ticket_for_device(
                    row.data["device_id"]
                )
                if ticket_id:
                    row.data["ticket_id"] = ticket_id
                    logger.debug(
                        f"Enriched biosensor data with ticket_id: "
                        f"{row.data['device_id']} → {ticket_id}"
                    )
                else:
                    logger.debug(
                        f"No ticket_id mapping found for device_id: "
                        f"{row.data['device_id']}"
                    )

        except Exception as e:
            logger.error(f"Error processing biosensor data: {e}")

    async def _add_blueiot_sensor_data(self, row: RowInProgress,
                                       event: RedisDataEvent) -> None:
        """Process BlueIoT sensor data (position) from the event."""
        try:
            # Parse BlueIoT JSON data
            if isinstance(event.value, str):
                try:
                    data = json.loads(event.value)
                except json.JSONDecodeError:
                    return
            elif isinstance(event.value, dict):
                data = event.value
            else:
                return

            # Get field mappings for blueiot table
            field_mappings = self._field_mappings_cache.get("blueiot", {})
            
            # Apply field mappings parametrically
            for source_field, target_field in field_mappings.items():
                if source_field in data:
                    # For device_id: allow overwrite if it's None or empty (from failed regex extraction)
                    # For other fields: don't overwrite existing data
                    should_set = target_field not in row.data or (
                        target_field == "device_id" and not row.data.get("device_id")
                    )
                    
                    if should_set:
                        # Special handling for position array
                        if source_field == "BlueIoT_Position" and isinstance(data[source_field], list):
                            pos = data[source_field]
                            if len(pos) >= 1:
                                row.data["position_x"] = float(pos[0])
                            if len(pos) >= 2:
                                row.data["position_y"] = float(pos[1])
                        else:
                            # For other fields, try to convert appropriately
                            try:
                                row.data[target_field] = float(data[source_field])
                            except (ValueError, TypeError):
                                # If conversion fails, store as string
                                row.data[target_field] = str(data[source_field])
            
            # Enrich with ticket_id from device_id mapping
            if self.device_mapper and "device_id" in row.data:
                ticket_id = await self.device_mapper.get_ticket_for_device(
                    row.data["device_id"]
                )
                if ticket_id:
                    row.data["ticket_id"] = ticket_id
                    logger.debug(
                        f"Enriched BlueIoT data with ticket_id: "
                        f"{row.data['device_id']} → {ticket_id}"
                    )
                else:
                    logger.debug(
                        f"No ticket_id mapping found for device_id: "
                        f"{row.data['device_id']}"
                    )

        except Exception as e:
            logger.error(f"Error processing BlueIoT data: {e}")

    async def _add_global_state_data(self, row: RowInProgress,
                                     event: RedisDataEvent) -> None:
        """Add global state event data to the row."""
        # Set required fields first (before any potential failures)
        row.data["state_key"] = event.key_or_channel
        row.data["event_source_type"] = event.source_type
        
        # Store state_value as JSON string for human readability
        # Complex types (lists, dicts) are JSON-encoded for proper parsing
        # Simple types (strings, numbers) are stored as plain strings
        try:
            if event.value is None:
                row.data["state_value"] = None
            elif isinstance(event.value, msgpack.ext.Timestamp):
                # Convert msgpack Timestamp to datetime, then to ISO string
                row.data["state_value"] = event.value.to_datetime().isoformat()
            elif isinstance(event.value, (datetime, date, pd.Timestamp)):
                # Convert timestamps to ISO format strings
                row.data["state_value"] = event.value.isoformat()
            elif isinstance(event.value, (dict, list)):
                # JSON-encode complex types with custom serializer for timestamps
                # Ensures proper format like ["a", "b"] not "['a', 'b']"
                row.data["state_value"] = json.dumps(event.value, default=_json_serial)
            elif isinstance(event.value, bool):
                # Encode booleans as JSON "true"/"false" for proper parsing
                row.data["state_value"] = json.dumps(event.value)
            elif isinstance(event.value, (int, float)):
                # Store numbers as strings for consistent handling
                row.data["state_value"] = str(event.value)
            else:
                # For strings, store as-is
                row.data["state_value"] = str(event.value)
        except Exception as e:
            # If value encoding fails, store a safe fallback
            logger.error(f"Failed to encode state_value for key {event.key_or_channel}: {e}")
            row.data["state_value"] = f"<encoding error: {type(event.value).__name__}>"
        
        # Extract ticket_id for Visitor events
        if event.key_or_channel.startswith("Visitors:"):
            parts = event.key_or_channel.split(":")
            if len(parts) >= 2:
                ticket_id = parts[1]
                row.data["ticket_id"] = ticket_id
                logger.debug(
                    f"Extracted ticket_id from key: {event.key_or_channel} → {ticket_id}"
                )
        
        # Add TTL if present (only for key events, not channel events)
        if event.ttl is not None:
            row.data["ttl_seconds"] = event.ttl

    def finalize_nested_structures(self, row: RowInProgress) -> None:
        """No-op in simplified pipeline."""
        pass