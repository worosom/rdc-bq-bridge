"""Test row assembly functionality."""

import asyncio
import pytest
from unittest.mock import Mock

from src.config import Config, RoutingRule, TableConfig
from src.redis_ingestor import RedisDataEvent
from src.row_assembler import RowAssembler
from src.row_models import AssembledRow


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    config = Mock(spec=Config)
    
    # Create mock BigQuery config with field mappings
    mock_bigquery = Mock()
    mock_table_config = Mock(spec=TableConfig)
    mock_table_config.field_mappings = {
        "Heart_Rate_Value": "heart_rate",
        "Skin_Conductivity_Value": "skin_conductivity",
        "Skin_Temperature_Value": "skin_temperature"
    }
    mock_bigquery.tables = {
        "empatica": mock_table_config
    }
    config.bigquery = mock_bigquery
    
    return config


@pytest.fixture
def routing_rule():
    """Create a routing rule for testing."""
    import re
    rule = RoutingRule(
        name="BioSensors",
        redis_source_type="channel",
        redis_pattern="Wearables:WatchDevices:*:BioSensors",
        target_table="empatica",
        id_parser_regex="Wearables:WatchDevices:(?P<device_id>[^:]+):BioSensors",
        value_is_object=True
    )
    rule.compiled_regex = re.compile(rule.id_parser_regex)
    return rule


@pytest.fixture
def global_state_key_routing_rule():
    """Create a routing rule for global state key events."""
    import re
    rule = RoutingRule(
        name="Visitors",
        redis_source_type="key",
        redis_pattern="Visitors:*",
        target_table="global_state_events",
        id_parser_regex="Visitors:(?P<ticket_id>[^:]+):.*",
        value_is_object=False
    )
    rule.compiled_regex = re.compile(rule.id_parser_regex)
    return rule


@pytest.fixture
def global_state_channel_routing_rule():
    """Create a routing rule for global state channel events."""
    import re
    rule = RoutingRule(
        name="VisitorRegister",
        redis_source_type="channel",
        redis_pattern="Visitors:Register",
        target_table="global_state_events",
        id_parser_regex="(?P<state_key>.*)",
        value_is_object=True
    )
    rule.compiled_regex = re.compile(rule.id_parser_regex)
    return rule


@pytest.mark.asyncio
async def test_extract_row_id(mock_config, routing_rule):
    """Test row ID extraction from Redis events."""
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()
    
    assembler = RowAssembler(mock_config, input_queue, output_queue)
    
    # Create test event
    event = RedisDataEvent(
        source_type="channel",
        key_or_channel="Wearables:WatchDevices:device123:BioSensors",
        value={"Heart_Rate_Value": 75},
        routing_rule=routing_rule
    )
    
    # Extract row ID
    row_id = assembler.row_processor.extract_row_id(event)
    
    assert row_id == "device123"


@pytest.mark.asyncio
async def test_biosensor_data_processing(mock_config, routing_rule):
    """Test processing of biosensor data."""
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()
    
    assembler = RowAssembler(mock_config, input_queue, output_queue)
    
    # Create test event with biosensor data
    event = RedisDataEvent(
        source_type="channel",
        key_or_channel="Wearables:WatchDevices:device123:BioSensors",
        value={
            "Heart_Rate_Value": 75.5,
            "Skin_Conductivity_Value": 0.8,
            "Skin_Temperature_Value": 36.2
        },
        routing_rule=routing_rule
    )
    
    # Process the event
    await input_queue.put(event)
    
    # Start assembler task
    assembler_task = asyncio.create_task(assembler.start())
    
    # Wait a bit for processing
    await asyncio.sleep(0.1)
    
    # Stop assembler
    await assembler.stop()
    assembler_task.cancel()
    
    # Check output
    assert not output_queue.empty()
    assembled_row = await output_queue.get()
    
    assert assembled_row.target_table == "empatica"
    assert assembled_row.data["device_id"] == "device123"
    assert assembled_row.data["heart_rate"] == 75.5
    assert assembled_row.data["skin_conductivity"] == 0.8
    assert assembled_row.data["skin_temperature"] == 36.2


@pytest.mark.asyncio
async def test_global_state_key_event_source_type(mock_config, global_state_key_routing_rule):
    """Test that key events have event_source_type='key' in global_state_events."""
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()
    
    assembler = RowAssembler(mock_config, input_queue, output_queue)
    
    # Create test event for a key invalidation
    event = RedisDataEvent(
        source_type="key",
        key_or_channel="Visitors:TICKET123:Status",
        value="Active",
        routing_rule=global_state_key_routing_rule,
        ttl=None  # Key without TTL
    )
    
    # Process the event
    await input_queue.put(event)
    
    # Start assembler task
    assembler_task = asyncio.create_task(assembler.start())
    
    # Wait a bit for processing
    await asyncio.sleep(0.1)
    
    # Stop assembler
    await assembler.stop()
    assembler_task.cancel()
    
    # Check output
    assert not output_queue.empty()
    assembled_row = await output_queue.get()
    
    assert assembled_row.target_table == "global_state_events"
    assert assembled_row.data["state_key"] == "Visitors:TICKET123:Status"
    assert assembled_row.data["state_value"] == "Active"
    assert assembled_row.data["event_source_type"] == "key"
    assert assembled_row.data["ticket_id"] == "TICKET123"
    assert "ttl_seconds" not in assembled_row.data  # TTL was None


@pytest.mark.asyncio
async def test_global_state_channel_event_source_type(mock_config, global_state_channel_routing_rule):
    """Test that channel events have event_source_type='channel' in global_state_events."""
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()
    
    assembler = RowAssembler(mock_config, input_queue, output_queue)
    
    # Create test event for a channel message
    event = RedisDataEvent(
        source_type="channel",
        key_or_channel="Visitors:Register",
        value={"ticket_id": "TICKET456", "status": "Registered"},
        routing_rule=global_state_channel_routing_rule,
        ttl=None  # Channel events don't have TTL
    )
    
    # Process the event
    await input_queue.put(event)
    
    # Start assembler task
    assembler_task = asyncio.create_task(assembler.start())
    
    # Wait a bit for processing
    await asyncio.sleep(0.1)
    
    # Stop assembler
    await assembler.stop()
    assembler_task.cancel()
    
    # Check output
    assert not output_queue.empty()
    assembled_row = await output_queue.get()
    
    assert assembled_row.target_table == "global_state_events"
    assert assembled_row.data["state_key"] == "Visitors:Register"
    assert assembled_row.data["event_source_type"] == "channel"
    assert "ttl_seconds" not in assembled_row.data  # Channel events don't have TTL