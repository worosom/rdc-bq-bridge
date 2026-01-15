#!/usr/bin/env python3
"""
Test the complete pipeline flow from Redis to BigQuery.
This will identify exactly where the data flow is breaking.
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
from src.bq_loader import BigQueryLoaderManager

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_complete_flow():
    """Test the complete data flow from Redis to BigQuery."""
    logger.info("=" * 60)
    logger.info("TESTING COMPLETE PIPELINE FLOW")
    logger.info("=" * 60)
    
    config = load_config(get_default_config_path())
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    # Create queues
    redis_to_assembler_queue = asyncio.Queue()
    assembler_to_loader_queue = asyncio.Queue()
    
    # Create components
    ingestor = RedisIngestor(config, redis_to_assembler_queue)
    assembler = RowAssembler(config, redis_to_assembler_queue, assembler_to_loader_queue)
    loader_manager = BigQueryLoaderManager(config, assembler_to_loader_queue)
    
    try:
        # Start all components
        logger.info("\n1. Starting all pipeline components...")
        ingestor_task = asyncio.create_task(ingestor.start())
        assembler_task = asyncio.create_task(assembler.start())
        loader_task = asyncio.create_task(loader_manager.start())
        
        # Wait for initialization
        await asyncio.sleep(3)
        
        # Generate test data
        logger.info("\n2. Generating test data...")
        test_devices = ["pipeline_test_001", "pipeline_test_002"]
        
        for device_id in test_devices:
            # Publish BioSensor data to channel
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
            
            num_subs = await redis_client.publish(biosensor_channel, json.dumps(biosensor_data))
            logger.info(f"  Published BioSensor data for {device_id} ({num_subs} subscribers)")
            
            # Set Position data as key
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
            logger.info(f"  Set Position data for {device_id}")
            
            await asyncio.sleep(0.5)
        
        # Also add some global state data
        await redis_client.set("System:TestCounter", "100")
        logger.info("  Set global state data")
        
        # Wait for processing
        logger.info("\n3. Waiting for data to flow through pipeline...")
        await asyncio.sleep(8)  # Wait longer than row assembly timeout
        
        # Check queue states
        logger.info("\n4. Checking queue states:")
        logger.info(f"  Redis->Assembler queue: {redis_to_assembler_queue.qsize()} items")
        logger.info(f"  Assembler->Loader queue: {assembler_to_loader_queue.qsize()} items")
        
        # Get statistics
        logger.info("\n5. Component statistics:")
        assembler_stats = assembler.get_stats()
        loader_stats = loader_manager.get_stats()
        logger.info(f"  Assembler: {assembler_stats}")
        logger.info(f"  Loader: {loader_stats}")
        
        # Stop components
        logger.info("\n6. Stopping components...")
        await loader_manager.stop()
        await assembler.stop()
        await ingestor.stop()
        
        # Cancel tasks
        for task in [ingestor_task, assembler_task, loader_task]:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Check BigQuery for data
        logger.info("\n7. Checking BigQuery for data...")
        bq_client = bigquery.Client()
        
        # Check visitor_events
        query = f"""
        SELECT COUNT(*) as count
        FROM `dataland-backend.dl_staging.visitor_events`
        WHERE device_id LIKE 'pipeline_test_%'
        """
        
        result = bq_client.query(query).result()
        visitor_count = list(result)[0].count
        logger.info(f"  Visitor events in BigQuery: {visitor_count}")
        
        # Check global_state_events
        query = f"""
        SELECT COUNT(*) as count
        FROM `dataland-backend.dl_staging.global_state_events`
        WHERE state_key = 'System:TestCounter'
        """
        
        result = bq_client.query(query).result()
        global_count = list(result)[0].count
        logger.info(f"  Global state events in BigQuery: {global_count}")
        
        # Analysis
        logger.info("\n" + "=" * 60)
        logger.info("ANALYSIS")
        logger.info("=" * 60)
        
        if visitor_count > 0:
            logger.info("✅ Visitor events are flowing to BigQuery!")
        else:
            logger.error("❌ Visitor events are NOT reaching BigQuery")
            
        if global_count > 0:
            logger.info("✅ Global state events are flowing to BigQuery!")
        else:
            logger.error("❌ Global state events are NOT reaching BigQuery")
        
        return visitor_count > 0
        
    except Exception as e:
        logger.error(f"Error in test: {e}", exc_info=True)
        return False
    finally:
        await redis_client.close()


async def test_direct_row_insertion():
    """Test inserting rows directly into the BigQuery loader queue."""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING DIRECT ROW INSERTION")
    logger.info("=" * 60)
    
    config = load_config(get_default_config_path())
    loader_queue = asyncio.Queue()
    
    # Create test rows
    test_row = AssembledRow(
        target_table="visitor_events",
        data={
            "device_id": "direct_test_001",
            "event_timestamp": datetime.utcnow(),
            "heart_rate": 80.0,
            "position_x": 100.0,
            "position_y": 200.0,
            "zone": "DirectTestZone"
        }
    )
    
    # Add to queue
    await loader_queue.put(test_row)
    logger.info(f"Added test row to queue")
    
    # Create and start loader
    loader_manager = BigQueryLoaderManager(config, loader_queue)
    loader_task = asyncio.create_task(loader_manager.start())
    
    # Wait for processing
    await asyncio.sleep(5)
    
    # Check statistics
    stats = loader_manager.get_stats()
    logger.info(f"Loader statistics: {stats}")
    
    # Stop loader
    await loader_manager.stop()
    loader_task.cancel()
    try:
        await loader_task
    except asyncio.CancelledError:
        pass
    
    # Check BigQuery
    bq_client = bigquery.Client()
    query = f"""
    SELECT COUNT(*) as count
    FROM `dataland-backend.dl_staging.visitor_events`
    WHERE device_id = 'direct_test_001'
    """
    
    result = bq_client.query(query).result()
    count = list(result)[0].count
    
    if count > 0:
        logger.info("✅ Direct insertion works - rows can reach BigQuery")
    else:
        logger.error("❌ Direct insertion failed - issue with BigQuery loader")
    
    return count > 0


async def main():
    """Main test function."""
    logger.info("=" * 60)
    logger.info("COMPLETE PIPELINE TEST")
    logger.info("=" * 60)
    
    # Test complete flow
    complete_flow_ok = await test_complete_flow()
    
    # If complete flow fails, test direct insertion
    if not complete_flow_ok:
        direct_ok = await test_direct_row_insertion()
        
        if direct_ok:
            logger.info("\n🔍 DIAGNOSIS: Issue is between assembler and loader")
            logger.info("Rows can be written to BigQuery, but aren't flowing through the pipeline")
        else:
            logger.info("\n🔍 DIAGNOSIS: Issue is with BigQuery loader or permissions")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    if complete_flow_ok:
        logger.info("✅ PIPELINE IS WORKING!")
        logger.info("Visitor events are now flowing correctly to BigQuery")
    else:
        logger.info("❌ PIPELINE STILL HAS ISSUES")
        logger.info("Check the analysis above for specific problems")


if __name__ == "__main__":
    asyncio.run(main())

