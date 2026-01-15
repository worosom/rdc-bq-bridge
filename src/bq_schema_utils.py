"""BigQuery schema utilities for dynamic protobuf generation."""

import logging
from datetime import datetime
from typing import Any, Type, TYPE_CHECKING

from google.cloud import bigquery
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message
from google.protobuf.message_factory import GetMessageClass

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


# Global config instance (set by initialize_schemas)
_config: 'Config | None' = None


def initialize_schemas(config: 'Config') -> None:
    """Initialize the schema utilities with the config.
    
    This must be called before using get_table_schema and related functions.
    """
    global _config
    _config = config
    logger.info("Schema utilities initialized with config")


def get_table_schema(table_name: str) -> list[bigquery.SchemaField]:
    """Get the schema for a table from config."""
    if not _config:
        raise RuntimeError("Schema utilities not initialized. Call initialize_schemas(config) first.")
    
    if not _config.bigquery:
        raise ValueError("BigQuery configuration section missing in config.yaml")
    
    if table_name not in _config.bigquery.tables:
        raise ValueError(f"Unknown table: {table_name}. Available tables: {list(_config.bigquery.tables.keys())}")
    
    table_config = _config.bigquery.tables[table_name]
    schema_fields = []
    
    for field in table_config.schema:
        schema_fields.append(
            bigquery.SchemaField(
                name=field.name,
                field_type=field.type,
                mode=field.mode,
                description=field.description
            )
        )
    
    return schema_fields


def get_table_partitioning(table_name: str) -> bigquery.TimePartitioning | None:
    """Get the partitioning configuration for a table."""
    if not _config or not _config.bigquery:
        raise RuntimeError("Schema utilities not initialized or BigQuery config missing")
    
    if table_name not in _config.bigquery.tables:
        raise ValueError(f"Unknown table: {table_name}")
    
    table_config = _config.bigquery.tables[table_name]
    partition_type = table_config.partitioning_type
    
    # Map string to BigQuery enum
    type_mapping = {
        "DAY": bigquery.TimePartitioningType.DAY,
        "HOUR": bigquery.TimePartitioningType.HOUR,
        "MONTH": bigquery.TimePartitioningType.MONTH,
        "YEAR": bigquery.TimePartitioningType.YEAR,
    }
    
    return bigquery.TimePartitioning(
        type_=type_mapping.get(partition_type, bigquery.TimePartitioningType.DAY),
        field=table_config.partitioning_field
    )


def get_table_clustering_fields(table_name: str) -> list[str] | None:
    """Get the clustering fields for a table."""
    if not _config or not _config.bigquery:
        raise RuntimeError("Schema utilities not initialized or BigQuery config missing")
    
    if table_name not in _config.bigquery.tables:
        raise ValueError(f"Unknown table: {table_name}")
    
    table_config = _config.bigquery.tables[table_name]
    return table_config.clustering_fields if table_config.clustering_fields else None


def get_table_description(table_name: str) -> str | None:
    """Get the description for a table."""
    if not _config or not _config.bigquery:
        raise RuntimeError("Schema utilities not initialized or BigQuery config missing")
    
    if table_name not in _config.bigquery.tables:
        raise ValueError(f"Unknown table: {table_name}")
    
    table_config = _config.bigquery.tables[table_name]
    return table_config.description


def create_proto_descriptor(bq_schema: list[bigquery.SchemaField], table_name: str) -> tuple[Descriptor, descriptor_pb2.DescriptorProto]:
    """Create a protobuf descriptor from a BigQuery schema.

    Returns:
        tuple: (Descriptor object, DescriptorProto object for BigQuery Storage Write API)
    """

    # Create file descriptor proto
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = f"{table_name}.proto"
    file_proto.package = "rdc_bridge"

    # Create main message descriptor proto
    main_message_proto = descriptor_pb2.DescriptorProto()
    main_message_proto.name = f"{table_name}_row"

    # Track nested message types that need to be added
    nested_messages = {}

    # Add fields from BigQuery schema
    field_number = 1
    for field in bq_schema:
        field_proto = main_message_proto.field.add()
        field_proto.number = field_number
        field_proto.name = field.name

        # Handle nested RECORD types
        if field.field_type == "RECORD":
            # Create a nested message type for this record
            nested_msg_name = f"{field.name}_type"
            nested_msg_proto = descriptor_pb2.DescriptorProto()
            nested_msg_proto.name = nested_msg_name

            # Add fields to the nested message
            nested_field_number = 1
            for nested_field in field.fields:
                nested_field_proto = nested_msg_proto.field.add()
                nested_field_proto.number = nested_field_number
                nested_field_proto.name = nested_field.name

                # Map BigQuery types to protobuf types for nested fields
                if nested_field.field_type == "STRING":
                    nested_field_proto.type = FieldDescriptor.TYPE_STRING
                elif nested_field.field_type in ["INTEGER", "INT64"]:
                    nested_field_proto.type = FieldDescriptor.TYPE_INT64
                elif nested_field.field_type in ["FLOAT", "FLOAT64"]:
                    nested_field_proto.type = FieldDescriptor.TYPE_DOUBLE
                elif nested_field.field_type == "BOOLEAN":
                    nested_field_proto.type = FieldDescriptor.TYPE_BOOL
                elif nested_field.field_type == "TIMESTAMP":
                    nested_field_proto.type = FieldDescriptor.TYPE_STRING  # Store as ISO string
                elif nested_field.field_type == "DATETIME":
                    nested_field_proto.type = FieldDescriptor.TYPE_STRING  # Store as ISO string
                else:
                    # Default to string for unknown types
                    nested_field_proto.type = FieldDescriptor.TYPE_STRING

                # Set field mode for nested field
                if nested_field.mode == "REQUIRED":
                    nested_field_proto.label = FieldDescriptor.LABEL_REQUIRED
                elif nested_field.mode == "REPEATED":
                    nested_field_proto.label = FieldDescriptor.LABEL_REPEATED
                else:  # NULLABLE
                    nested_field_proto.label = FieldDescriptor.LABEL_OPTIONAL

                nested_field_number += 1

            # Store the nested message to add it later
            nested_messages[nested_msg_name] = nested_msg_proto

            # Set the field type to MESSAGE and reference the nested type
            field_proto.type = FieldDescriptor.TYPE_MESSAGE
            # For nested types, use just the type name without package prefix
            # Since the nested type is defined within the same message
            field_proto.type_name = nested_msg_name

            # Add the nested message as a nested type of the main message
            main_message_proto.nested_type.append(nested_msg_proto)

        else:
            # Handle non-nested field types
            if field.field_type == "STRING":
                field_proto.type = FieldDescriptor.TYPE_STRING
            elif field.field_type in ["INTEGER", "INT64"]:
                field_proto.type = FieldDescriptor.TYPE_INT64
            elif field.field_type in ["FLOAT", "FLOAT64"]:
                field_proto.type = FieldDescriptor.TYPE_DOUBLE
            elif field.field_type == "BOOLEAN":
                field_proto.type = FieldDescriptor.TYPE_BOOL
            elif field.field_type == "TIMESTAMP":
                field_proto.type = FieldDescriptor.TYPE_STRING  # Store as ISO string
            elif field.field_type == "DATETIME":
                field_proto.type = FieldDescriptor.TYPE_STRING  # Store as ISO string
            else:
                # Default to string for unknown types
                field_proto.type = FieldDescriptor.TYPE_STRING

        # Set field mode (for non-nested fields)
        if field.field_type != "RECORD":
            if field.mode == "REQUIRED":
                field_proto.label = FieldDescriptor.LABEL_REQUIRED
            elif field.mode == "REPEATED":
                field_proto.label = FieldDescriptor.LABEL_REPEATED
            else:  # NULLABLE
                field_proto.label = FieldDescriptor.LABEL_OPTIONAL
        else:
            # For RECORD types, set the label based on mode
            if field.mode == "REPEATED":
                field_proto.label = FieldDescriptor.LABEL_REPEATED
            else:
                field_proto.label = FieldDescriptor.LABEL_OPTIONAL

        field_number += 1

    # Add the main message to the file
    file_proto.message_type.append(main_message_proto)

    # Create descriptor from proto
    try:
        from google.protobuf.descriptor_pool import DescriptorPool
        pool = DescriptorPool()
        file_desc = pool.Add(file_proto)
        descriptor = file_desc.message_types_by_name[main_message_proto.name]
    except Exception as e:
        logger.error(f"Error creating descriptor from pool: {e}")
        # Fallback: create descriptor directly
        from google.protobuf.descriptor import MakeDescriptor
        descriptor = MakeDescriptor(main_message_proto)

    # Return descriptor and DescriptorProto object
    return descriptor, main_message_proto


def create_proto_message(descriptor: Descriptor, data: dict[str, Any], table_name: str) -> Message:
    """Create a protobuf message instance from data and descriptor."""

    # Create message class dynamically
    message_class = _create_message_class(descriptor)

    # Create message instance
    message_instance = message_class()

    # Populate fields
    for field_name, field_value in data.items():
        if field_value is None:
            continue

        # Get field descriptor
        field_desc = descriptor.fields_by_name.get(field_name)
        if not field_desc:
            logger.warning(
                f"Field {field_name} not found in descriptor for {table_name}")
            continue

        try:
            # Handle nested messages (RECORD types)
            if field_desc.type == FieldDescriptor.TYPE_MESSAGE:
                if isinstance(field_value, dict):
                    # Get the nested message object (already created by protobuf)
                    nested_message = getattr(message_instance, field_name)

                    # Populate the nested message fields
                    _populate_nested_message(
                        nested_message, field_desc.message_type, field_value)
                else:
                    logger.warning(
                        f"Expected dict for nested field {field_name}, got {type(field_value)}")
            else:
                # Convert and set field value for non-nested fields
                converted_value = _convert_field_value(field_value, field_desc)
                setattr(message_instance, field_name, converted_value)

        except Exception as e:
            logger.error(
                f"Error setting field {field_name} for {table_name}: {e}")
            # Continue with other fields
            continue

    return message_instance


def _populate_nested_message(nested_message: Message, nested_desc: Descriptor, data: dict[str, Any]) -> None:
    """Populate fields in a nested message."""

    for nested_field_name, nested_field_value in data.items():
        if nested_field_value is None:
            continue

        # Get nested field descriptor
        nested_field_desc = nested_desc.fields_by_name.get(nested_field_name)
        if not nested_field_desc:
            logger.warning(
                f"Nested field {nested_field_name} not found in descriptor")
            continue

        try:
            # Handle nested messages within nested messages (if needed)
            if nested_field_desc.type == FieldDescriptor.TYPE_MESSAGE:
                if isinstance(nested_field_value, dict):
                    # Recursively populate nested message
                    sub_nested_message = getattr(
                        nested_message, nested_field_name)
                    _populate_nested_message(
                        sub_nested_message, nested_field_desc.message_type, nested_field_value)
                else:
                    logger.warning(
                        f"Expected dict for nested field {nested_field_name}, got {type(nested_field_value)}")
            else:
                # Convert and set nested field value
                converted_value = _convert_field_value(
                    nested_field_value, nested_field_desc)
                setattr(nested_message, nested_field_name, converted_value)
        except Exception as e:
            logger.error(
                f"Error setting nested field {nested_field_name}: {e}")
            continue


def _create_message_class(descriptor: Descriptor) -> Type[Message]:
    """Create a message class from a descriptor."""
    # Use GetMessageClass instead of deprecated MessageFactory
    message_class = GetMessageClass(descriptor)
    return message_class


def _convert_field_value(value: Any, field_desc: FieldDescriptor) -> Any:
    """Convert a Python value to the appropriate protobuf field type."""

    if value is None:
        return None

    field_type = field_desc.type

    try:
        if field_type == FieldDescriptor.TYPE_STRING:
            if isinstance(value, datetime):
                return value.isoformat()
            return str(value)

        elif field_type == FieldDescriptor.TYPE_INT64:
            return int(value)

        elif field_type == FieldDescriptor.TYPE_DOUBLE:
            return float(value)

        elif field_type == FieldDescriptor.TYPE_BOOL:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)

        else:
            # Default to string conversion
            return str(value)

    except (ValueError, TypeError) as e:
        logger.warning(
            f"Error converting value {value} for field type {field_type}: {e}")
        # Return string representation as fallback
        return str(value)