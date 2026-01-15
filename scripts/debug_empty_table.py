#!/usr/bin/env python3
"""
Debug script to trace why visitor_events table is empty.
This script will check each stage of the data pipeline.
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis.asyncio as redis
from google.cloud import bigquery

from src.config import load_config, get_default_config_path

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def check_redis_data():
    """Check what data is actually in Redis."""
    logger.info("=" * 60)
    logger.info("STAGE 1: CHECKING REDIS DATA")
    logger.info("=" * 60)
    
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    try:
        # Check all keys in Redis
        all_keys = await redis_client.keys("*")
        logger.info(f"Total keys in Redis: {len(all_keys)}")
        
        if all_keys:
            logger.info("\nAll Redis keys:")
            for key in all_keys[:20]:  # Show first 20 keys
                key_str = key.decode() if isinstance(key, bytes) else key
                key_type = await redis_client.type(key)
                type_str = key_type.decode() if isinstance(key_type, bytes) else key_type
                logger.info(f"  - {key_str} (type: {type_str})")
        
        # Check for patterns matching our config
        patterns_to_check = [
            "Wearables:WatchDevices:*:BioSensors",
            "Wearables:WatchDevices:*:Position",
            "visitor:*",
            "device:*",
            "device_*"
        ]
        
        logger.info("\nChecking for expected patterns:")
        for pattern in patterns_to_check:
            matching_keys = await redis_client.keys(pattern)
            logger.info(f"  Pattern '{pattern}': {len(matching_keys)} matches")
            if matching_keys:
                for key in matching_keys[:3]:  # Show first 3 matches
                    key_str = key.decode() if isinstance(key, bytes) else key
                    value = await redis_client.get(key)
                    if value:
                        value_str = value.decode() if isinstance(value, bytes) else str(value)
                        logger.info(f"    - {key_str}: {value_str[:100]}...")
        
        # Check Redis channels (pub/sub)
        logger.info("\nChecking Redis Pub/Sub channels:")
        pubsub = redis_client.pubsub()
        
        # Try to subscribe to expected channels
        channel_pattern = "Wearables:WatchDevices:*:BioSensors"
        await pubsub.psubscribe(channel_pattern)
        logger.info(f"  Subscribed to pattern: {channel_pattern}")
        
        # Check for any messages (with timeout)
        logger.info("  Waiting for messages (5 seconds)...")
        try:
            message = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout=5.0)
            if message:
                logger.info(f"    Received message: {message}")
            else:
                logger.info("    No messages received")
        except asyncio.TimeoutError:
            logger.info("    No messages received (timeout)")
        
        await pubsub.unsubscribe()
        await pubsub.close()
        
        return all_keys
        
    finally:
        await redis_client.close()


async def check_pipeline_config():
    """Check the pipeline configuration and routing rules."""
    logger.info("\n" + "=" * 60)
    logger.info("STAGE 2: CHECKING PIPELINE CONFIGURATION")
    logger.info("=" * 60)
    
    config = load_config(get_default_config_path())
    
    logger.info("\nRouting rules:")
    for rule in config.pipeline.routing_rules:
        logger.info(f"\n  Rule: {rule.name}")
        logger.info(f"    Source type: {rule.redis_source_type}")
        logger.info(f"    Pattern: {rule.redis_pattern}")
        logger.info(f"    Target table: {rule.target_table}")
        logger.info(f"    ID parser regex: {rule.id_parser_regex}")
        logger.info(f"    Value is object: {rule.value_is_object}")
    
    return config


async def simulate_data_flow(config):
    """Simulate the data flow to identify where it breaks."""
    logger.info("\n" + "=" * 60)
    logger.info("STAGE 3: SIMULATING DATA FLOW")
    logger.info("=" * 60)
    
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    try:
        # Create test data that should match our routing rules
        test_data = {
            "heart_rate": 75.5,
            "skin_conductivity": 0.8,
            "skin_temperature": 36.5,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Test 1: Key-based routing (Position)
        position_key = "Wearables:WatchDevices:test_device_001:Position"
        position_data = {"x": 100.5, "y": 200.3, "timestamp": datetime.utcnow().isoformat()}
        
        logger.info(f"\nTest 1: Setting key '{position_key}'")
        await redis_client.set(position_key, json.dumps(position_data))
        
        # Verify it was set
        value = await redis_client.get(position_key)
        logger.info(f"  Value set: {value.decode() if value else 'None'}")
        
        # Test 2: Channel-based routing (BioSensors)
        channel = "Wearables:WatchDevices:test_device_001:BioSensors"
        logger.info(f"\nTest 2: Publishing to channel '{channel}'")
        subscribers = await redis_client.publish(channel, json.dumps(test_data))
        logger.info(f"  Published to {subscribers} subscribers")
        
        # Test 3: Create data that matches visitor_events schema exactly
        visitor_event_data = {
            "device_id": "test_device_001",
            "ticket_id": "TICKET_001",
            "position_x": 100.5,
            "position_y": 200.3,
            "zone": "Zone_A",
            "heart_rate": 75.5,
            "skin_conductivity": 0.8,
            "skin_temperature": 36.5,
            "accelerometer_x": 0.1,
            "accelerometer_y": 0.2,
            "accelerometer_z": 0.3,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Try different key formats
        test_keys = [
            ("visitor:test_device_001", visitor_event_data),
            ("device:test_device_001:data", visitor_event_data),
            ("Wearables:WatchDevices:test_device_001:All", visitor_event_data)
        ]
        
        logger.info("\nTest 3: Setting various key formats with complete visitor data:")
        for key, data in test_keys:
            await redis_client.set(key, json.dumps(data))
            logger.info(f"  Set key: {key}")
        
        return True
        
    finally:
        await redis_client.close()


async def check_bigquery_data():
    """Check if any data made it to BigQuery."""
    logger.info("\n" + "=" * 60)
    logger.info("STAGE 4: CHECKING BIGQUERY DATA")
    logger.info("=" * 60)
    
    client = bigquery.Client()
    
    # Check visitor_events table
    query = """
    SELECT 
        COUNT(*) as total_rows,
        MIN(event_timestamp) as earliest_event,
        MAX(event_timestamp) as latest_event
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    """
    
    try:
        result = client.query(query).result()
        for row in result:
            logger.info(f"\nvisitor_events table:")
            logger.info(f"  Total rows: {row.total_rows}")
            logger.info(f"  Earliest event: {row.earliest_event}")
            logger.info(f"  Latest event: {row.latest_event}")
    except Exception as e:
        logger.error(f"Error querying visitor_events: {e}")
    
    # Check global_state_events table
    query = """
    SELECT 
        COUNT(*) as total_rows,
        MIN(event_timestamp) as earliest_event,
        MAX(event_timestamp) as latest_event
    FROM 
        `dataland-backend.dl_staging.global_state_events`
    """
    
    try:
        result = client.query(query).result()
        for row in result:
            logger.info(f"\nglobal_state_events table:")
            logger.info(f"  Total rows: {row.total_rows}")
            logger.info(f"  Earliest event: {row.earliest_event}")
            logger.info(f"  Latest event: {row.latest_event}")
    except Exception as e:
        logger.error(f"Error querying global_state_events: {e}")
    
    # Check for any recent streaming buffer data
    query = """
    SELECT 
        COUNT(*) as streaming_buffer_rows
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    WHERE 
        _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    """
    
    try:
        result = client.query(query).result()
        for row in result:
            logger.info(f"\nStreaming buffer (today):")
            logger.info(f"  Rows in buffer: {row.streaming_buffer_rows}")
    except Exception as e:
        logger.debug(f"Could not check streaming buffer: {e}")


async def test_direct_pipeline():
    """Test the pipeline components directly."""
    logger.info("\n" + "=" * 60)
    logger.info("STAGE 5: TESTING PIPELINE COMPONENTS DIRECTLY")
    logger.info("=" * 60)
    
    from src.redis_ingestor import RedisIngestor, RedisDataEvent
    from src.row_assembler import RowAssembler, AssembledRow
    import re
    
    config = load_config(get_default_config_path())
    
    # Create queues
    redis_queue = asyncio.Queue()
    assembler_queue = asyncio.Queue()
    
    # Test data matching the BioSensors pattern
    test_channel = "Wearables:WatchDevices:test_device_debug:BioSensors"
    test_data = {
        "heart_rate": 80,
        "skin_conductivity": 0.9,
        "skin_temperature": 37.0,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    logger.info(f"\nTesting with channel: {test_channel}")
    logger.info(f"Data: {json.dumps(test_data, indent=2)}")
    
    # Find matching routing rule
    matching_rule = None
    for rule in config.pipeline.routing_rules:
        if rule.redis_source_type == "channel":
            # Convert glob pattern to regex
            pattern = rule.redis_pattern.replace("*", ".*")
            if re.match(pattern, test_channel):
                matching_rule = rule
                logger.info(f"\nMatched routing rule: {rule.name}")
                break
    
    if not matching_rule:
        logger.error("No matching routing rule found!")
        return
    
    # Extract device_id using the regex
    match = re.match(matching_rule.id_parser_regex, test_channel)
    if match:
        device_id = match.group('device_id')
        logger.info(f"Extracted device_id: {device_id}")
    else:
        logger.error(f"Could not extract device_id from channel name")
        return
    
    # Create RedisDataEvent
    redis_event = RedisDataEvent(
        source_type="channel",
        key=test_channel,
        value=json.dumps(test_data),
        routing_rule=matching_rule,
        timestamp=datetime.utcnow()
    )
    
    logger.info(f"\nCreated RedisDataEvent:")
    logger.info(f"  Source type: {redis_event.source_type}")
    logger.info(f"  Key: {redis_event.key}")
    logger.info(f"  Target table: {redis_event.routing_rule.target_table}")
    
    # Test row assembly
    await redis_queue.put(redis_event)
    
    # Process through row assembler
    assembler = RowAssembler(config, redis_queue, assembler_queue)
    
    # Manually call the processing method
    logger.info("\nProcessing through RowAssembler...")
    
    # Check what the assembler would create
    row_data = {}
    
    # Parse the value
    if matching_rule.value_is_object:
        try:
            parsed_value = json.loads(redis_event.value)
            logger.info(f"Parsed value: {parsed_value}")
            
            # Map to BigQuery schema fields
            row_data['device_id'] = device_id
            row_data['event_timestamp'] = datetime.utcnow().isoformat()
            
            # Map bio sensor data
            if 'heart_rate' in parsed_value:
                row_data['heart_rate'] = parsed_value['heart_rate']
            if 'skin_conductivity' in parsed_value:
                row_data['skin_conductivity'] = parsed_value['skin_conductivity']
            if 'skin_temperature' in parsed_value:
                row_data['skin_temperature'] = parsed_value['skin_temperature']
            
            logger.info(f"Assembled row data: {json.dumps(row_data, indent=2)}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
    
    # Create AssembledRow
    assembled_row = AssembledRow(
        table_name=matching_rule.target_table,
        row_data=row_data
    )
    
    logger.info(f"\nCreated AssembledRow:")
    logger.info(f"  Table: {assembled_row.table_name}")
    logger.info(f"  Data: {assembled_row.row_data}")
    
    return assembled_row


async def main():
    """Main debugging function."""
    logger.info("=" * 60)
    logger.info("DEBUGGING EMPTY VISITOR_EVENTS TABLE")
    logger.info("=" * 60)
    
    # Stage 1: Check Redis data
    redis_keys = await check_redis_data()
    
    # Stage 2: Check pipeline config
    config = await check_pipeline_config()
    
    # Stage 3: Simulate data flow
    await simulate_data_flow(config)
    
    # Stage 4: Check BigQuery
    await check_bigquery_data()
    
    # Stage 5: Test pipeline components
    assembled_row = await test_direct_pipeline()
    
    # Diagnosis
    logger.info("\n" + "=" * 60)
    logger.info("DIAGNOSIS")
    logger.info("=" * 60)
    
    issues_found = []
    
    if not redis_keys or len(redis_keys) == 0:
        issues_found.append("❌ No data in Redis")
    
    # Check if Redis patterns match config
    logger.info("\nPotential issues:")
    if issues_found:
        for issue in issues_found:
            logger.info(f"  {issue}")
    else:
        logger.info("  ✅ Redis has data")
    
    logger.info("\n🔍 LIKELY CAUSE:")
    logger.info("The Redis data patterns don't match the routing rules in the config.")
    logger.info("\n📝 RECOMMENDATIONS:")
    logger.info("1. Check that Redis data generator creates keys/channels matching these patterns:")
    logger.info("   - Channels: 'Wearables:WatchDevices:*:BioSensors'")
    logger.info("   - Keys: 'Wearables:WatchDevices:*:Position'")
    logger.info("2. Ensure the data generator publishes to channels (not just sets keys)")
    logger.info("3. Verify the value format is JSON with the expected fields")
    logger.info("4. Check that the pipeline is subscribing to the correct patterns")


if __name__ == "__main__":
    asyncio.run(main())

