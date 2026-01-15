#!/usr/bin/env python3
"""
Test script to verify BlueIoT position data processing.
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
from src.config import load_config, get_default_config_path
from src.redis_ingestor import RedisIngestor, RedisDataEvent
from src.row_assembler import RowAssembler, AssembledRow

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_blueiot_position_processing():
    """Test that BlueIoT position data is correctly processed."""
    logger.info("=" * 60)
    logger.info("TESTING BLUEIOT POSITION DATA PROCESSING")
    logger.info("=" * 60)

    redis_client = redis.Redis.from_url("redis://localhost:6379/0")

    try:
        # Load config and create components
        config = load_config(get_default_config_path())
        ingestor_output_queue = asyncio.Queue()
        assembler_output_queue = asyncio.Queue()

        # Create ingestor and assembler
        ingestor = RedisIngestor(config, ingestor_output_queue)
        assembler = RowAssembler(config, ingestor_output_queue, assembler_output_queue)

        # Start both components
        ingestor_task = asyncio.create_task(ingestor.start())
        assembler_task = asyncio.create_task(assembler.start())

        # Give them time to initialize
        await asyncio.sleep(2)

        # Test 1: BlueIoTSensors channel with position data
        logger.info("\nTest 1: Publishing to BlueIoTSensors channel with position data")

        device_id = "test_blueiot_001"
        channel = f"Wearables:WatchDevices:{device_id}:BlueIoTSensors"

        test_data = {
            "BlueIoT_Position": [10.169974327087402, 5.250168800354004],
            "Room_ID": 1,
            "device_id": device_id,
            "ticket_id": "TICKET_BT_001"
        }

        subscribers = await redis_client.publish(channel, json.dumps(test_data))
        logger.info(f"Published to {channel} ({subscribers} subscribers)")
        logger.info(f"Data: {json.dumps(test_data, indent=2)}")

        # Wait for processing
        await asyncio.sleep(3)

        # Test 2: Send biosensor data for the same device
        logger.info("\nTest 2: Publishing BioSensors data for same device")

        bio_channel = f"Wearables:WatchDevices:{device_id}:BioSensors"
        bio_data = {
            "Heart_Rate_Value": 75,
            "Skin_Conductivity_Value": 0.8,
            "Skin_Temperature_Value": 36.5,
            "device_id": device_id,
            "ticket_id": "TICKET_BT_001"
        }

        subscribers = await redis_client.publish(bio_channel, json.dumps(bio_data))
        logger.info(f"Published to {bio_channel} ({subscribers} subscribers)")
        logger.info(f"Data: {json.dumps(bio_data, indent=2)}")

        # Wait for timeout to trigger row finalization
        logger.info("\nWaiting for row assembly timeout (5 seconds)...")
        await asyncio.sleep(6)

        # Check assembled rows
        logger.info(f"\nAssembler output queue size: {assembler_output_queue.qsize()}")

        assembled_rows = []
        while not assembler_output_queue.empty():
            row = await assembler_output_queue.get()
            assembled_rows.append(row)
            logger.info(f"\n✅ Assembled row for table: {row.target_table}")
            logger.info(f"   Row data: {json.dumps(row.data, indent=4, default=str)}")

            # Verify position data was correctly extracted
            if "position_x" in row.data and "position_y" in row.data:
                logger.info(f"   ✓ Position extracted: x={row.data['position_x']}, y={row.data['position_y']}")
            else:
                logger.warning(f"   ✗ Position data missing!")

            if "zone" in row.data:
                logger.info(f"   ✓ Zone extracted: {row.data['zone']}")
            else:
                logger.warning(f"   ✗ Zone data missing!")

        # Test 3: Regular Position channel with BlueIoT format
        logger.info("\nTest 3: Publishing to Position channel with BlueIoT format")

        device_id2 = "test_position_002"
        pos_channel = f"Wearables:WatchDevices:{device_id2}:Position"

        pos_data = {
            "BlueIoT_Position": [20.5, 30.7],
            "Room_ID": 2,
            "device_id": device_id2,
            "ticket_id": "TICKET_POS_002"
        }

        subscribers = await redis_client.publish(pos_channel, json.dumps(pos_data))
        logger.info(f"Published to {pos_channel} ({subscribers} subscribers)")
        logger.info(f"Data: {json.dumps(pos_data, indent=2)}")

        # Wait for timeout
        await asyncio.sleep(6)

        # Check assembled rows again
        logger.info(f"\nChecking for Position channel row...")
        while not assembler_output_queue.empty():
            row = await assembler_output_queue.get()
            assembled_rows.append(row)
            logger.info(f"\n✅ Assembled row for table: {row.target_table}")
            logger.info(f"   Row data: {json.dumps(row.data, indent=4, default=str)}")

            if row.data.get("device_id") == device_id2:
                if "position_x" in row.data and "position_y" in row.data:
                    logger.info(f"   ✓ Position channel with BlueIoT format works!")
                else:
                    logger.warning(f"   ✗ Position data missing from Position channel!")

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

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)

        if assembled_rows:
            logger.info(f"✅ Successfully assembled {len(assembled_rows)} rows")

            # Check if all rows have position data
            rows_with_position = sum(1 for r in assembled_rows
                                    if "position_x" in r.data and "position_y" in r.data)
            logger.info(f"   {rows_with_position}/{len(assembled_rows)} rows have position data")

            rows_with_zone = sum(1 for r in assembled_rows if "zone" in r.data)
            logger.info(f"   {rows_with_zone}/{len(assembled_rows)} rows have zone data")
        else:
            logger.warning("⚠️ No rows were assembled")

        return len(assembled_rows) > 0

    finally:
        await redis_client.close()


async def main():
    """Main test function."""
    success = await test_blueiot_position_processing()

    if not success:
        logger.error("\n❌ BlueIoT position processing test failed!")
        sys.exit(1)
    else:
        logger.info("\n✅ All tests passed!")


if __name__ == "__main__":
    asyncio.run(main())