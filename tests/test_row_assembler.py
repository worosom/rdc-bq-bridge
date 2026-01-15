"""Test row assembly functionality."""

import asyncio
import pytest
from unittest.mock import Mock

from src.config import Config, RoutingRule, AssemblyConfig
from src.redis_ingestor import RedisDataEvent
from src.row_assembler import RowAssembler, AssembledRow


@pytest.fixture
def mock_config():
    """Create a mock configuration for testing."""
    config = Mock(spec=Config)
    config.assembly = AssemblyConfig(
        row_assembly_timeout_seconds=0.1,
        aggregation_rules={}
    )
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