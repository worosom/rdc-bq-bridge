"""File format writers for BigQuery exports."""

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Define Avro schema for replay format (reusable across methods)
REDIS_REPLAY_AVRO_SCHEMA = {
    "type": "record",
    "name": "RedisEvent",
    "namespace": "com.dataland.rdc.replay",
    "fields": [
        {"name": "timestamp", "type": "long", "doc": "Event timestamp (milliseconds since epoch)"},
        {"name": "type", "type": {"type": "enum", "name": "EventType", "symbols": ["channel", "key"]}, "doc": "Redis event type"},
        {"name": "key", "type": "string", "doc": "Redis key or channel name"},
        {"name": "value", "type": "bytes", "doc": "Event value (msgpack-encoded bytes)"},
        {"name": "ttl", "type": ["null", "long"], "default": None, "doc": "TTL in seconds at capture time (null = no expiry or channel event, positive = TTL)"}
    ]
}


class FormatWriter:
    """Handles conversion of DataFrames to various file formats."""
    
    def __init__(self, table_configs: dict[str, Any] = None):
        """
        Initialize FormatWriter with optional table configurations.
        
        Args:
            table_configs: Dictionary mapping table names to TableConfig objects
                          containing field_mappings and field_types
        """
        self.table_configs = table_configs or {}
    
    def _save_avro_schema(self, schema: dict, output_path: Path) -> None:
        """
        Save Avro schema as a .avsc file.
        
        Args:
            schema: Avro schema dictionary
            output_path: Path for the .avsc file
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(schema, f, indent=2)
            logger.info(f"Saved Avro schema to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save Avro schema: {e}")
            raise
    
    def write_parquet(
        self,
        df: pd.DataFrame,
        output_path: Path,
        compression: str = "snappy",
        row_group_size: int = 100000
    ) -> None:
        """
        Write DataFrame to Parquet format.
        
        Parquet is optimal for data scientists and analytical queries.
        It's columnar, highly compressed, and preserves BigQuery types.
        
        Args:
            df: DataFrame to write
            output_path: Output file path
            compression: Compression codec (snappy, gzip, zstd)
            row_group_size: Rows per row group
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            logger.info(f"Writing Parquet file to {output_path} with {len(df)} rows")
            
            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Write with specified settings
            pq.write_table(
                table,
                output_path,
                compression=compression,
                row_group_size=row_group_size,
                use_dictionary=True,  # Enable dictionary encoding for strings
                write_statistics=True,  # Enable statistics for query optimization
            )
            
            file_size = output_path.stat().st_size
            logger.info(f"Successfully wrote Parquet file ({file_size:,} bytes)")
            
        except ImportError:
            logger.error("pyarrow is not installed. Run: pip install pyarrow")
            raise
        except Exception as e:
            logger.error(f"Failed to write Parquet file: {e}")
            raise
    
    def write_avro_for_replay(
        self,
        df: pd.DataFrame,
        output_path: Path,
        codec: str = "deflate"
    ) -> None:
        """
        Write DataFrame to Avro format optimized for Redis replay.
        
        Transforms the data into a streaming format:
        | timestamp | type | key | value |
        
        Where:
        - timestamp: Event timestamp
        - type: "channel" or "key" (Redis event type)
        - key: Redis key or channel name
        - value: The actual value (msgpack-encoded bytes for objects, string for primitives)
        
        Args:
            df: DataFrame to write
            output_path: Output file path
            codec: Avro compression codec (null, deflate, snappy)
        """
        try:
            import fastavro
            
            logger.info(f"Writing Avro file for Redis replay to {output_path}")
            
            # Transform DataFrame to replay format
            replay_records = self._transform_to_replay_format(df)
            
            # Write Avro file
            with open(output_path, 'wb') as out:
                fastavro.writer(
                    out,
                    REDIS_REPLAY_AVRO_SCHEMA,
                    replay_records,
                    codec=codec
                )
            
            file_size = output_path.stat().st_size
            logger.info(f"Successfully wrote Avro file with {len(replay_records)} events ({file_size:,} bytes)")
            
            # Save schema as .avsc file
            schema_path = output_path.with_suffix('.avsc')
            self._save_avro_schema(REDIS_REPLAY_AVRO_SCHEMA, schema_path)
            
        except ImportError:
            logger.error("fastavro is not installed. Run: pip install fastavro")
            raise
        except Exception as e:
            logger.error(f"Failed to write Avro file: {e}")
            raise
    
    def write_combined_avro_for_replay(
        self,
        dataframes: dict[str, pd.DataFrame],
        output_path: Path,
        codec: str = "deflate"
    ) -> None:
        """
        Write multiple DataFrames to a single Avro file ordered by timestamp.
        
        This combines all tables into one AVRO file, preserving the temporal
        order of events for accurate Redis replay. Values are encoded as msgpack.
        
        Args:
            dataframes: Dictionary mapping table names to DataFrames
            output_path: Output file path
            codec: Avro compression codec (null, deflate, snappy)
        """
        try:
            import fastavro
            
            logger.info(f"Writing combined Avro file for Redis replay to {output_path}")
            
            # Transform all DataFrames to replay format
            all_records = []
            for table_name, df in dataframes.items():
                logger.info(f"Processing {len(df)} records from {table_name}")
                records = self._transform_to_replay_format(df)
                all_records.extend(records)
            
            # Sort all records by timestamp to preserve order
            logger.info(f"Sorting {len(all_records)} total records by timestamp")
            all_records.sort(key=lambda x: x["timestamp"])
            
            # Write Avro file
            with open(output_path, 'wb') as out:
                fastavro.writer(
                    out,
                    REDIS_REPLAY_AVRO_SCHEMA,
                    all_records,
                    codec=codec
                )
            
            file_size = output_path.stat().st_size
            logger.info(f"Successfully wrote combined Avro file with {len(all_records)} events ({file_size:,} bytes)")
            
            # Save schema as .avsc file
            schema_path = output_path.with_suffix('.avsc')
            self._save_avro_schema(REDIS_REPLAY_AVRO_SCHEMA, schema_path)
            
        except ImportError:
            logger.error("fastavro is not installed. Run: pip install fastavro")
            raise
        except Exception as e:
            logger.error(f"Failed to write combined Avro file: {e}")
            raise
    
    def _transform_to_replay_format(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """
        Transform BigQuery data to Redis replay format.
        
        This is the critical transformation that converts structured BigQuery
        tables back into the original Redis event stream format.
        Values are encoded as msgpack for efficient binary serialization.
        
        Returns list of records with: timestamp, type, key, value (msgpack bytes)
        """
        import msgpack
        
        replay_records = []
        
        # Detect which table we're dealing with based on columns
        columns = set(df.columns)
        
        if "state_key" in columns:
            # global_state_events table
            for _, row in df.iterrows():
                # For simple string values, msgpack encode the string
                value_str = str(row["state_value"])
                
                # Extract TTL if present
                ttl = None
                if "ttl_seconds" in row and pd.notna(row["ttl_seconds"]):
                    ttl = int(row["ttl_seconds"])
                
                replay_records.append({
                    "timestamp": int(row["event_timestamp"].timestamp() * 1000),
                    "type": "key",  # These are Redis key events
                    "key": row["state_key"],
                    "value": msgpack.packb(value_str, use_bin_type=True),
                    "ttl": ttl
                })
        
        elif "device_id" in columns and "heart_rate" in columns:
            # empatica table - channel events
            # Get field mappings and types for empatica table
            empatica_config = self.table_configs.get("empatica", {})
            field_mappings = getattr(empatica_config, 'field_mappings', {})
            field_types = getattr(empatica_config, 'field_types', {})
            
            # Create reverse mapping: BQ column name -> Redis field name
            reverse_mappings = {v: k for k, v in field_mappings.items() if k != "device_id"}
            
            for _, row in df.iterrows():
                device_id = row["device_id"]
                channel_name = f"Wearables:WatchDevices:{device_id}:BioSensors"
                
                # Build biosensor object using field mappings and types
                biosensor_data = {}
                
                for bq_column, redis_field in reverse_mappings.items():
                    if pd.notna(row.get(bq_column)):
                        # Get the target type for this Redis field
                        target_type = field_types.get(redis_field, "float")
                        
                        # Apply type conversion
                        if target_type == "int":
                            # Convert to float first to handle strings like "100.0", then to int
                            biosensor_data[redis_field] = int(float(row[bq_column]))
                        elif target_type == "float":
                            biosensor_data[redis_field] = float(row[bq_column])
                        else:  # str
                            biosensor_data[redis_field] = str(row[bq_column])
                
                replay_records.append({
                    "timestamp": int(row["event_timestamp"].timestamp() * 1000),
                    "type": "channel",
                    "key": channel_name,
                    "value": msgpack.packb(biosensor_data, use_bin_type=True),
                    "ttl": None  # Channel events don't have TTL
                })
        
        elif "device_id" in columns and "position_x" in columns:
            # blueiot table - channel events
            # Get field mappings and types for blueiot table
            blueiot_config = self.table_configs.get("blueiot", {})
            field_types = getattr(blueiot_config, 'field_types', {})
            
            # Get position type (default to float)
            position_type = field_types.get("BlueIoT_Position", "float")
            room_type = field_types.get("Room_ID", "str")
            
            for _, row in df.iterrows():
                device_id = row["device_id"]
                channel_name = f"Wearables:WatchDevices:{device_id}:BlueIoTSensors"
                
                # Build position object with configured types
                position_x = 0
                position_y = 0
                
                if pd.notna(row.get("position_x")):
                    position_x = float(row["position_x"]) if position_type == "float" else int(row["position_x"])
                if pd.notna(row.get("position_y")):
                    position_y = float(row["position_y"]) if position_type == "float" else int(row["position_y"])
                
                position_data = {
                    "BlueIoT_Position": [position_x, position_y]
                }
                
                if pd.notna(row.get("room")):
                    if room_type == "int":
                        # Convert to float first to handle strings like "0.0", then to int
                        position_data["Room_ID"] = int(float(row["room"]))
                    elif room_type == "float":
                        position_data["Room_ID"] = float(row["room"])
                    else:
                        position_data["Room_ID"] = str(row["room"])
                
                replay_records.append({
                    "timestamp": int(row["event_timestamp"].timestamp() * 1000),
                    "type": "channel",
                    "key": channel_name,
                    "value": msgpack.packb(position_data, use_bin_type=True),
                    "ttl": None  # Channel events don't have TTL
                })
        
        else:
            logger.warning(f"Unknown table format with columns: {columns}")
        
        # Sort by timestamp to maintain event order
        replay_records.sort(key=lambda x: x["timestamp"])
        
        return replay_records
    
    def write_jsonl(
        self,
        df: pd.DataFrame,
        output_path: Path
    ) -> None:
        """
        Write DataFrame to JSON Lines format.
        
        Each line is a complete JSON object. This format is less efficient
        than Parquet or Avro but is human-readable and universally supported.
        
        Args:
            df: DataFrame to write
            output_path: Output file path
        """
        try:
            logger.info(f"Writing JSONL file to {output_path}")
            
            df.to_json(
                output_path,
                orient="records",
                lines=True,
                date_format="iso"
            )
            
            file_size = output_path.stat().st_size
            logger.info(f"Successfully wrote JSONL file ({file_size:,} bytes)")
            
        except Exception as e:
            logger.error(f"Failed to write JSONL file: {e}")
            raise