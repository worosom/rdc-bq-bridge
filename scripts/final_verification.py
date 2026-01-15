#!/usr/bin/env python3
"""
Final verification script to ensure visitor_events are now flowing to BigQuery.
This script will generate data, run the pipeline, and verify the results.
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def generate_comprehensive_test_data():
    """Generate comprehensive test data for all event types."""
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    try:
        logger.info("Generating comprehensive test data...")
        
        # Generate data for multiple devices
        test_devices = [f"final_test_{i:03d}" for i in range(1, 6)]
        
        for device_id in test_devices:
            # 1. BioSensor data (channel)
            biosensor_channel = f"Wearables:WatchDevices:{device_id}:BioSensors"
            biosensor_data = {
                "heart_rate": 70 + (hash(device_id) % 30),
                "skin_conductivity": 0.5 + (hash(device_id) % 10) * 0.1,
                "skin_temperature": 35.5 + (hash(device_id) % 20) * 0.1,
                "accelerometer_x": (hash(device_id) % 10) * 0.1,
                "accelerometer_y": (hash(device_id) % 10) * 0.1,
                "accelerometer_z": 9.8,
                "device_id": device_id,
                "ticket_id": f"TICKET_{device_id}",
                "timestamp": datetime.utcnow().isoformat()
            }
            
            num_subs = await redis_client.publish(biosensor_channel, json.dumps(biosensor_data))
            logger.info(f"  Published BioSensor data for {device_id} ({num_subs} subscribers)")
            
            # 2. Position data (key)
            position_key = f"Wearables:WatchDevices:{device_id}:Position"
            position_data = {
                "position_x": (hash(device_id) % 100),
                "position_y": (hash(device_id + "y") % 100),
                "zone": f"Zone_{chr(65 + hash(device_id) % 5)}",  # Zone_A through Zone_E
                "device_id": device_id,
                "ticket_id": f"TICKET_{device_id}",
                "timestamp": datetime.utcnow().isoformat()
            }
            
            await redis_client.set(position_key, json.dumps(position_data))
            logger.info(f"  Set Position data for {device_id}")
            
            await asyncio.sleep(0.2)  # Small delay between devices
        
        # 3. Global state data
        global_states = [
            ("System:FinalTest:TotalVisitors", str(len(test_devices))),
            ("System:FinalTest:Timestamp", datetime.utcnow().isoformat()),
            ("Rooms:MainHall:FinalTest", "active")
        ]
        
        for key, value in global_states:
            await redis_client.set(key, value)
            logger.info(f"  Set global state: {key}")
        
        logger.info(f"✅ Generated test data for {len(test_devices)} devices")
        return len(test_devices)
        
    finally:
        await redis_client.close()


async def run_pipeline_and_wait():
    """Run the pipeline for a specific duration."""
    logger.info("\nStarting pipeline to process data...")
    
    # Import here to avoid circular imports
    from src.main import RDCBigQueryBridge
    from src.config import load_config, get_default_config_path
    
    config = load_config(get_default_config_path())
    bridge = RDCBigQueryBridge(config)
    
    # Run the pipeline
    pipeline_task = asyncio.create_task(bridge.start())
    
    try:
        # Let it run for 20 seconds to ensure all data is processed
        logger.info("Pipeline running (20 seconds)...")
        await asyncio.sleep(20)
        
    finally:
        # Stop the pipeline
        logger.info("Stopping pipeline...")
        pipeline_task.cancel()
        try:
            await pipeline_task
        except asyncio.CancelledError:
            pass
        await bridge.stop()


def verify_bigquery_data(expected_devices):
    """Verify that data made it to BigQuery."""
    logger.info("\nVerifying data in BigQuery...")
    
    client = bigquery.Client()
    results = {}
    
    # Check visitor_events
    query = """
    SELECT 
        COUNT(DISTINCT device_id) as unique_devices,
        COUNT(*) as total_rows,
        MIN(event_timestamp) as earliest,
        MAX(event_timestamp) as latest,
        ARRAY_AGG(DISTINCT zone IGNORE NULLS) as zones,
        AVG(heart_rate) as avg_heart_rate
    FROM 
        `dataland-backend.dl_staging.visitor_events`
    WHERE 
        device_id LIKE 'final_test_%'
        AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """
    
    try:
        result = client.query(query).result()
        for row in result:
            results['visitor_events'] = {
                'unique_devices': row.unique_devices,
                'total_rows': row.total_rows,
                'zones': list(row.zones) if row.zones else [],
                'avg_heart_rate': row.avg_heart_rate
            }
            
            logger.info(f"\n📊 Visitor Events:")
            logger.info(f"  Unique devices: {row.unique_devices}/{expected_devices}")
            logger.info(f"  Total rows: {row.total_rows}")
            logger.info(f"  Zones visited: {', '.join(row.zones) if row.zones else 'None'}")
            logger.info(f"  Avg heart rate: {row.avg_heart_rate:.1f}" if row.avg_heart_rate else "  No heart rate data")
            
    except Exception as e:
        logger.error(f"Error querying visitor_events: {e}")
        results['visitor_events'] = {'error': str(e)}
    
    # Check global_state_events
    query = """
    SELECT 
        COUNT(DISTINCT state_key) as unique_keys,
        COUNT(*) as total_rows,
        ARRAY_AGG(DISTINCT state_key) as keys
    FROM 
        `dataland-backend.dl_staging.global_state_events`
    WHERE 
        state_key LIKE '%FinalTest%'
        AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """
    
    try:
        result = client.query(query).result()
        for row in result:
            results['global_state_events'] = {
                'unique_keys': row.unique_keys,
                'total_rows': row.total_rows,
                'keys': list(row.keys) if row.keys else []
            }
            
            logger.info(f"\n📊 Global State Events:")
            logger.info(f"  Unique keys: {row.unique_keys}")
            logger.info(f"  Total rows: {row.total_rows}")
            if row.keys:
                logger.info(f"  Keys: {', '.join(row.keys[:5])}")  # Show first 5 keys
                
    except Exception as e:
        logger.error(f"Error querying global_state_events: {e}")
        results['global_state_events'] = {'error': str(e)}
    
    return results


async def main():
    """Main verification function."""
    logger.info("=" * 70)
    logger.info("FINAL VERIFICATION: VISITOR EVENTS TO BIGQUERY")
    logger.info("=" * 70)
    logger.info("\nThis test will:")
    logger.info("1. Generate test data for multiple devices")
    logger.info("2. Run the complete pipeline")
    logger.info("3. Verify data arrives in BigQuery")
    logger.info("")
    
    start_time = time.time()
    
    # Step 1: Generate test data
    logger.info("\n" + "-" * 50)
    logger.info("STEP 1: GENERATING TEST DATA")
    logger.info("-" * 50)
    num_devices = await generate_comprehensive_test_data()
    
    # Step 2: Run pipeline
    logger.info("\n" + "-" * 50)
    logger.info("STEP 2: RUNNING PIPELINE")
    logger.info("-" * 50)
    await run_pipeline_and_wait()
    
    # Step 3: Wait for BigQuery writes
    logger.info("\nWaiting 5 seconds for BigQuery writes to complete...")
    await asyncio.sleep(5)
    
    # Step 4: Verify results
    logger.info("\n" + "-" * 50)
    logger.info("STEP 3: VERIFYING RESULTS")
    logger.info("-" * 50)
    results = verify_bigquery_data(num_devices)
    
    # Final summary
    elapsed_time = time.time() - start_time
    logger.info("\n" + "=" * 70)
    logger.info("FINAL RESULTS")
    logger.info("=" * 70)
    
    visitor_success = False
    global_success = False
    
    if 'visitor_events' in results and 'unique_devices' in results['visitor_events']:
        visitor_count = results['visitor_events']['unique_devices']
        if visitor_count > 0:
            logger.info(f"\n✅ SUCCESS: Visitor events are working!")
            logger.info(f"   {visitor_count} devices successfully sent data to BigQuery")
            visitor_success = True
        else:
            logger.error(f"\n❌ FAILURE: No visitor events in BigQuery")
    
    if 'global_state_events' in results and 'unique_keys' in results['global_state_events']:
        global_count = results['global_state_events']['unique_keys']
        if global_count > 0:
            logger.info(f"\n✅ SUCCESS: Global state events are working!")
            logger.info(f"   {global_count} keys successfully sent data to BigQuery")
            global_success = True
        else:
            logger.error(f"\n❌ FAILURE: No global state events in BigQuery")
    
    logger.info(f"\n⏱️  Total test time: {elapsed_time:.1f} seconds")
    
    if visitor_success and global_success:
        logger.info("\n🎉 ALL TESTS PASSED! The pipeline is working correctly.")
        logger.info("   Visitor events are now flowing from Redis to BigQuery!")
        return True
    else:
        logger.error("\n⚠️  Some tests failed. Check the logs above for details.")
        if not visitor_success:
            logger.info("\n📝 Troubleshooting visitor events:")
            logger.info("   1. Check that Redis pub/sub is enabled")
            logger.info("   2. Verify the channel patterns match")
            logger.info("   3. Ensure the row assembler timeout is appropriate")
            logger.info("   4. Check BigQuery permissions and schema")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nTest failed with error: {e}")
        sys.exit(1)

