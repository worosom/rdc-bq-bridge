#!/usr/bin/env python3
"""
Script to diagnose and fix the visitor_events processing issue.
This will identify why visitor_events aren't reaching BigQuery while global_state_events are.
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
from src.redis_ingestor import RedisIngestor, RedisDataEvent
from src.row_assembler import RowAssembler, AssembledRow

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_channel_processing():
    """Test if channel messages are being processed correctly."""
    logger.info("=" * 60)
    logger.info("TESTING CHANNEL MESSAGE PROCESSING")
    logger.info("=" * 60)
    
    config = load_config(get_default_config_path())
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    # Create queues
    redis_queue = asyncio.Queue()
    assembler_queue = asyncio.Queue()
    
    # Create components
    ingestor = RedisIngestor(config, redis_queue)
    assembler = RowAssembler(config, redis_queue, assembler_queue)
    
    try:
        # Start the ingestor
        logger.info("\nStarting Redis ingestor...")
        ingestor_task = asyncio.create_task(ingestor.start())
        
        # Start the assembler
        logger.info("Starting row assembler...")
        assembler_task = asyncio.create_task(assembler.start())
        
        # Wait for components to initialize
        await asyncio.sleep(2)
        
        # Test 1: Publish BioSensor data to channel
        logger.info("\nTest 1: Publishing BioSensor data to channel...")
        device_id = "test_fix_001"
        biosensor_channel = f"Wearables:WatchDevices:{device_id}:BioSensors"
        biosensor_data = {
            "heart_rate": 75.5,
            "skin_conductivity": 0.8,
            "skin_temperature": 36.5,
            "accelerometer_x": 0.1,
            "accelerometer_y": 0.2,
            "accelerometer_z": 9.8,
            "device_id": device_id,
            "ticket_id": f"TICKET_{device_id}",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Check how many subscribers are listening
        num_subs = await redis_client.publish(biosensor_channel, json.dumps(biosensor_data))
        logger.info(f"Published to {biosensor_channel}")
        logger.info(f"Number of subscribers: {num_subs}")
        
        if num_subs == 0:
            logger.error("❌ No subscribers listening to the channel!")
            logger.info("This is the main issue - the channel subscriber is not running")
        
        # Wait for processing
        await asyncio.sleep(2)
        
        # Check queue sizes
        logger.info(f"\nRedis queue size: {redis_queue.qsize()}")
        logger.info(f"Assembler queue size: {assembler_queue.qsize()}")
        
        # Test 2: Also set Position data as a key
        logger.info("\nTest 2: Setting Position data as key...")
        position_key = f"Wearables:WatchDevices:{device_id}:Position"
        position_data = {
            "position_x": 50.5,
            "position_y": 75.3,
            "zone": "TestZone",
            "device_id": device_id,
            "ticket_id": f"TICKET_{device_id}",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await redis_client.set(position_key, json.dumps(position_data))
        logger.info(f"Set key: {position_key}")
        
        # Wait for the row assembler timeout (5 seconds)
        logger.info("\nWaiting for row assembly timeout (6 seconds)...")
        await asyncio.sleep(6)
        
        # Check final queue sizes
        logger.info(f"\nFinal Redis queue size: {redis_queue.qsize()}")
        logger.info(f"Final Assembler queue size: {assembler_queue.qsize()}")
        
        # Process assembled rows
        assembled_rows = []
        while not assembler_queue.empty():
            row = await assembler_queue.get()
            assembled_rows.append(row)
            logger.info(f"Assembled row for table {row.target_table}:")
            logger.info(f"  Fields: {list(row.data.keys())}")
            logger.info(f"  Device ID: {row.data.get('device_id', 'MISSING')}")
        
        # Stop components
        await ingestor.stop()
        await assembler.stop()
        
        ingestor_task.cancel()
        assembler_task.cancel()
        
        try:
            await ingestor_task
            await assembler_task
        except asyncio.CancelledError:
            pass
        
        # Analysis
        logger.info("\n" + "=" * 60)
        logger.info("ANALYSIS")
        logger.info("=" * 60)
        
        if num_subs == 0:
            logger.error("\n❌ PROBLEM IDENTIFIED: Channel subscriber is not running!")
            logger.info("The Redis ingestor is not subscribing to channels properly.")
            logger.info("This is why BioSensor data (sent via channels) is not being processed.")
            return False
        
        if assembled_rows:
            logger.info(f"\n✅ Successfully assembled {len(assembled_rows)} rows")
            for row in assembled_rows:
                if row.target_table == "visitor_events":
                    logger.info(f"  Visitor event row has {len(row.data)} fields")
            return True
        else:
            logger.error("\n❌ No rows were assembled")
            return False
            
    finally:
        await redis_client.close()


async def verify_channel_subscriber_fix():
    """Verify that the channel subscriber is actually starting."""
    logger.info("\n" + "=" * 60)
    logger.info("VERIFYING CHANNEL SUBSCRIBER")
    logger.info("=" * 60)
    
    config = load_config(get_default_config_path())
    
    # Check config for channel rules
    channel_rules = [r for r in config.pipeline.routing_rules if r.redis_source_type == "channel"]
    logger.info(f"\nChannel rules in config: {len(channel_rules)}")
    for rule in channel_rules:
        logger.info(f"  - {rule.name}: {rule.redis_pattern}")
    
    if not channel_rules:
        logger.error("❌ No channel rules found in config!")
        return False
    
    # Test that channel subscriber actually starts
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    try:
        # Create a pubsub connection
        pubsub = redis_client.pubsub()
        
        # Subscribe to the patterns
        for rule in channel_rules:
            await pubsub.psubscribe(rule.redis_pattern)
            logger.info(f"✅ Successfully subscribed to: {rule.redis_pattern}")
        
        # Test publishing
        test_channel = "Wearables:WatchDevices:verify_test:BioSensors"
        test_data = {"test": "data"}
        
        # Publish in background
        async def publish_test():
            await asyncio.sleep(0.5)
            return await redis_client.publish(test_channel, json.dumps(test_data))
        
        publish_task = asyncio.create_task(publish_test())
        
        # Try to receive
        message = await asyncio.wait_for(
            pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1),
            timeout=2.0
        )
        
        num_subs = await publish_task
        
        if message:
            logger.info(f"✅ Received message on channel")
        
        logger.info(f"Number of subscribers: {num_subs}")
        
        await pubsub.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error testing channel subscription: {e}")
        return False
    finally:
        await redis_client.close()


async def main():
    """Main diagnostic function."""
    logger.info("=" * 60)
    logger.info("VISITOR EVENTS FIX DIAGNOSTIC")
    logger.info("=" * 60)
    
    # Verify channel subscriber
    channel_ok = await verify_channel_subscriber_fix()
    
    # Test full processing
    processing_ok = await test_channel_processing()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("DIAGNOSTIC SUMMARY")
    logger.info("=" * 60)
    
    if channel_ok and processing_ok:
        logger.info("\n✅ Channel processing is working correctly!")
        logger.info("Visitor events should now flow to BigQuery.")
    else:
        logger.error("\n❌ Issues found with channel processing")
        logger.info("\nRECOMMENDED FIX:")
        logger.info("The channel subscriber task in RedisIngestor may not be starting properly.")
        logger.info("Check that:")
        logger.info("1. The _channel_subscriber method is being called")
        logger.info("2. The channel patterns are being subscribed to")
        logger.info("3. The pmessage handler is processing messages correctly")


if __name__ == "__main__":
    asyncio.run(main())

