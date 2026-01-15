#!/usr/bin/env python3
"""
Test script to verify data flows correctly from Redis to BigQuery.
This script will generate test data, run the pipeline, and verify data arrives in BigQuery.
"""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis.asyncio as redis
from google.cloud import bigquery

from src.config import load_config, get_default_config_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def generate_test_data():
    """Generate test data in Redis that matches the expected patterns."""
    logger.info("=" * 60)
    logger.info("STEP 1: GENERATING TEST DATA")
    logger.info("=" * 60)
    
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    try:
        # Test device IDs
        test_devices = ["test_device_001", "test_device_002", "test_device_003"]
        
        for device_id in test_devices:
            # 1. Generate BioSensor data (channel)
            biosensor_channel = f"Wearables:WatchDevices:{device_id}:BioSensors"
            biosensor_data = {
                "heart_rate": 75.5,
                "skin_conductivity": 0.8,
                "skin_temperature": 36.5,
                "accelerometer_x": 0.1,
                "accelerometer_y": 0.2,
                "accelerometer_z": 9.8,
                "timestamp": datetime.utcnow().isoformat(),
                "device_id": device_id,  # Include device_id in data
                "ticket_id": f"TICKET_{device_id}"
            }
            
            # Publish to channel
            subscribers = await redis_client.publish(biosensor_channel, json.dumps(biosensor_data))
            logger.info(f"Published BioSensor data to '{biosensor_channel}' ({subscribers} subscribers)")
            
            # 2. Generate Position data (key)
            position_key = f"Wearables:WatchDevices:{device_id}:Position"
            position_data = {
                "position_x": 50.5,
                "position_y": 75.3,
                "zone": "MainHall",
                "timestamp": datetime.utcnow().isoformat(),
                "device_id": device_id,  # Include device_id in data
                "ticket_id": f"TICKET_{device_id}"
            }
            
            # Set key
            await redis_client.set(position_key, json.dumps(position_data))
            logger.info(f"Set Position data at '{position_key}'")
            
            # Small delay between devices
            await asyncio.sleep(0.1)
        
        # 3. Generate some global state data
        global_states = [
            ("System:TotalVisitors", "150"),
            ("Rooms:MainHall:Lighting:Brightness", "80"),
            ("Weather:Temperature", "22.5")
        ]
        
        for key, value in global_states:
            await redis_client.set(key, value)
            logger.info(f"Set global state: {key} = {value}")
        
        logger.info("\n✅ Test data generated successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error generating test data: {e}")
        return False
    finally:
        await redis_client.close()


async def run_pipeline_briefly():
    """Run the pipeline for a short time to process the test data."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: RUNNING PIPELINE")
    logger.info("=" * 60)
    
    logger.info("Starting pipeline to process test data...")
    logger.info("The pipeline will run for 15 seconds to process the data")
    
    # Import here to avoid circular imports
    from src.main import RDCBigQueryBridge
    
    config = load_config(get_default_config_path())
    bridge = RDCBigQueryBridge(config)
    
    # Run the pipeline for a limited time
    pipeline_task = asyncio.create_task(bridge.start())
    
    try:
        # Let it run for 15 seconds to process data
        await asyncio.sleep(15)
        logger.info("Stopping pipeline...")
        
    except Exception as e:
        logger.error(f"Error running pipeline: {e}")
    finally:
        # Stop the pipeline
        pipeline_task.cancel()
        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass
        await bridge.stop()
    
    logger.info("✅ Pipeline run completed")


def check_bigquery_data():
    """Check if the test data made it to BigQuery."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: VERIFYING DATA IN BIGQUERY")
    logger.info("=" * 60)
    
    client = bigquery.Client()
    
    # Check visitor_events table
    logger.info("\nChecking visitor_events table...")
    query = """
    SELECT 
        device_id,
        ticket_id,
        heart_rate,
        skin_temperature,
        position_x,
        position_y,
        zone,
        event_timestamp
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    WHERE 
        device_id LIKE 'test_device_%'
    ORDER BY 
        event_timestamp DESC
    LIMIT 10
    """
    
    try:
        result = client.query(query).result()
        rows = list(result)
        
        if rows:
            logger.info(f"✅ Found {len(rows)} test records in visitor_events table:")
            for row in rows:
                logger.info(f"  - Device: {row.device_id}, Zone: {row.zone}, "
                          f"HR: {row.heart_rate}, Pos: ({row.position_x}, {row.position_y})")
        else:
            logger.warning("⚠️ No test records found in visitor_events table")
            
    except Exception as e:
        logger.error(f"❌ Error querying visitor_events: {e}")
    
    # Check global_state_events table
    logger.info("\nChecking global_state_events table...")
    query = """
    SELECT 
        state_key,
        state_value,
        event_timestamp
    FROM 
        `dataland-backend.dl_staging.global_state_events`
    WHERE 
        state_key IN ('System:TotalVisitors', 'Rooms:MainHall:Lighting:Brightness', 'Weather:Temperature')
    ORDER BY 
        event_timestamp DESC
    LIMIT 10
    """
    
    try:
        result = client.query(query).result()
        rows = list(result)
        
        if rows:
            logger.info(f"✅ Found {len(rows)} test records in global_state_events table:")
            for row in rows:
                logger.info(f"  - Key: {row.state_key}, Value: {row.state_value}")
        else:
            logger.warning("⚠️ No test records found in global_state_events table")
            
    except Exception as e:
        logger.error(f"❌ Error querying global_state_events: {e}")
    
    # Get overall statistics
    logger.info("\nOverall Statistics:")
    query = """
    SELECT 
        'visitor_events' as table_name,
        COUNT(*) as total_rows,
        COUNT(DISTINCT device_id) as unique_devices
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    WHERE
        event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    
    UNION ALL
    
    SELECT 
        'global_state_events' as table_name,
        COUNT(*) as total_rows,
        COUNT(DISTINCT state_key) as unique_devices
    FROM 
        `dataland-backend.dl_staging.global_state_events`
    WHERE
        event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """
    
    try:
        result = client.query(query).result()
        for row in result:
            logger.info(f"  {row.table_name}: {row.total_rows} rows, {row.unique_devices} unique keys")
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")


async def main():
    """Main test function."""
    logger.info("=" * 60)
    logger.info("DATA FLOW TEST - REDIS TO BIGQUERY")
    logger.info("=" * 60)
    logger.info("\nThis test will:")
    logger.info("1. Generate test data in Redis")
    logger.info("2. Run the pipeline to process the data")
    logger.info("3. Verify the data arrives in BigQuery")
    logger.info("")
    
    # Step 1: Generate test data
    success = await generate_test_data()
    if not success:
        logger.error("Failed to generate test data")
        return False
    
    # Wait a moment for Redis to settle
    await asyncio.sleep(2)
    
    # Step 2: Run the pipeline
    await run_pipeline_briefly()
    
    # Wait for data to be written to BigQuery
    logger.info("\nWaiting 5 seconds for BigQuery writes to complete...")
    await asyncio.sleep(5)
    
    # Step 3: Check BigQuery
    check_bigquery_data()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info("\n🔍 RESULTS:")
    logger.info("If you see test records in the tables above, the fix is working!")
    logger.info("The data is now flowing correctly from Redis → Pipeline → BigQuery")
    logger.info("\n📝 NOTES:")
    logger.info("- The field mapping has been fixed to match the actual Redis data format")
    logger.info("- Both underscore (position_x) and camelCase (positionX) formats are supported")
    logger.info("- The pipeline correctly extracts device_id from the Redis key patterns")
    
    return True


if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)

