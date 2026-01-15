#!/usr/bin/env python3
"""
Debug script to test Redis channel subscriptions and data flow.
"""

import asyncio
import json
import logging
import sys
from datetime import datetime, UTC
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis.asyncio as redis
from src.config import load_config, get_default_config_path
from src.redis_ingestor import RedisIngestor, RedisDataEvent

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_channel_subscription():
    """Test if channel subscriptions are working."""
    logger.info("=" * 60)
    logger.info("TESTING CHANNEL SUBSCRIPTIONS")
    logger.info("=" * 60)

    redis_client = redis.Redis.from_url("redis://localhost:6379/0")

    try:
        # Test 1: Direct pub/sub test with wildcards
        logger.info("\nTest 1: Direct Redis Pub/Sub with Wildcards")
        pubsub = redis_client.pubsub()

        # Test multiple wildcard patterns
        patterns = [
            "Wearables:WatchDevices:*:BioSensors",
            "Wearables:*:*:*",  # More general wildcard
            "Test:*"  # Simple wildcard pattern
        ]

        for pattern in patterns:
            await pubsub.psubscribe(pattern)
            logger.info(f"Subscribed to pattern: {pattern}")

        # Publish test messages to different channels
        test_messages = [
            ("Wearables:WatchDevices:test_debug:BioSensors", {
                "heart_rate": 75,
                "skin_conductivity": 0.8,
                "skin_temperature": 36.5,
                "timestamp": datetime.now(UTC).isoformat()
            }),
            ("Wearables:SmartBand:device_001:HeartRate", {
                "heart_rate": 80,
                "timestamp": datetime.now(UTC).isoformat()
            }),
            ("Test:Channel1", {
                "data": "test_data",
                "timestamp": datetime.now(UTC).isoformat()
            })
        ]

        # Publish in background
        async def publish_after_delay():
            await asyncio.sleep(1)
            for channel, data in test_messages:
                subscribers = await redis_client.publish(channel, json.dumps(data))
                logger.info(f"Published to {channel}, {subscribers} subscribers")
                await asyncio.sleep(0.1)

        publish_task = asyncio.create_task(publish_after_delay())

        # Try to receive messages
        logger.info("Waiting for messages (5 seconds)...")
        received_count = 0
        timeout_at = asyncio.get_event_loop().time() + 5.0

        while asyncio.get_event_loop().time() < timeout_at:
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1),
                    timeout=0.5
                )
                if message and message.get('type') == 'pmessage':
                    channel = message.get('channel', b'').decode('utf-8') if isinstance(message.get('channel'), bytes) else message.get('channel', '')
                    pattern = message.get('pattern', b'').decode('utf-8') if isinstance(message.get('pattern'), bytes) else message.get('pattern', '')

                    # Handle data decoding carefully
                    data_raw = message.get('data', b'')
                    if isinstance(data_raw, bytes):
                        try:
                            data = data_raw.decode('utf-8')
                        except UnicodeDecodeError:
                            data = str(data_raw)
                    else:
                        data = str(data_raw)

                    logger.info(f"✅ Received message on channel '{channel}' matching pattern '{pattern}': {data}")
                    received_count += 1
            except asyncio.TimeoutError:
                continue

        await publish_task

        if received_count > 0:
            logger.info(f"✅ Successfully received {received_count} messages via wildcards")
        else:
            logger.warning("⚠️ No messages received")

        await pubsub.aclose()

        # Test 2: Test with RedisIngestor
        logger.info("\nTest 2: Testing RedisIngestor with Wildcards")
        config = load_config(get_default_config_path())
        output_queue = asyncio.Queue()

        ingestor = RedisIngestor(config, output_queue)

        # Check if channel rules are detected via routing_manager
        logger.info(f"Channel rules found: {len(ingestor.routing_manager.channel_rules)}")
        for rule in ingestor.routing_manager.channel_rules:
            logger.info(f"  - {rule.name}: {rule.redis_pattern} (wildcards: {'*' in rule.redis_pattern or '?' in rule.redis_pattern})")

        # Start the ingestor
        ingestor_task = asyncio.create_task(ingestor.start())

        # Give it time to subscribe
        await asyncio.sleep(2)

        # Publish test data to channels matching wildcard patterns
        logger.info("\nPublishing test data to wildcard-matching channels...")
        test_channels = [
            # Matches "Wearables:WatchDevices:*:BioSensors"
            ("Wearables:WatchDevices:test_device_001:BioSensors", {
                "heart_rate": 70,
                "skin_conductivity": 0.7,
                "skin_temperature": 36.0,
                "device_id": "test_device_001",
                "timestamp": datetime.now(UTC).isoformat()
            }),
            ("Wearables:WatchDevices:test_device_002:BioSensors", {
                "heart_rate": 75,
                "skin_conductivity": 0.75,
                "skin_temperature": 36.2,
                "device_id": "test_device_002",
                "timestamp": datetime.now(UTC).isoformat()
            }),
            # Matches "Wearables:WatchDevices:*:BlueIoTSensors"
            ("Wearables:WatchDevices:test_device_003:BlueIoTSensors", {
                "position_x": 50.0,
                "position_y": 75.0,
                "zone": "MainHall",
                "device_id": "test_device_003",
                "timestamp": datetime.now(UTC).isoformat()
            }),
            # Should NOT match any pattern
            ("Wearables:SmartBand:device_004:HeartRate", {
                "heart_rate": 85,
                "timestamp": datetime.now(UTC).isoformat()
            })
        ]

        for channel, data in test_channels:
            subscribers = await redis_client.publish(channel, json.dumps(data))
            logger.info(f"Published to {channel} ({subscribers} subscribers)")
            await asyncio.sleep(0.1)

        # Check if events were queued
        await asyncio.sleep(2)
        logger.info(f"\nOutput queue size: {output_queue.qsize()}")

        # Process queued events
        events_received = []
        while not output_queue.empty():
            event = await output_queue.get()
            events_received.append(event)
            logger.info(f"  Event: {event.key_or_channel} -> {event.routing_rule.target_table}")

        # Stop the ingestor
        await ingestor.stop()
        ingestor_task.cancel()
        try:
            await ingestor_task
        except asyncio.CancelledError:
            pass

        if events_received:
            logger.info(f"\n✅ Successfully received {len(events_received)} events via wildcard patterns")
            # Verify we got the expected number (should be 3, not 4)
            expected_count = 3  # BioSensors x2 + BlueIoTSensors x1
            if len(events_received) == expected_count:
                logger.info(f"✅ Correct number of events received (filtered non-matching channels)")
            else:
                logger.warning(f"⚠️ Expected {expected_count} events but got {len(events_received)}")
        else:
            logger.warning("\n⚠️ No events received by ingestor")

        return len(events_received) > 0

    finally:
        await redis_client.aclose()


async def test_key_subscription():
    """Test if key subscriptions are working."""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING KEY SUBSCRIPTIONS")
    logger.info("=" * 60)

    redis_client = redis.Redis.from_url("redis://localhost:6379/0")

    try:
        # Enable keyspace notifications if not already enabled
        await redis_client.config_set("notify-keyspace-events", "KEA")
        logger.info("Enabled keyspace notifications")

        config = load_config(get_default_config_path())
        output_queue = asyncio.Queue()

        ingestor = RedisIngestor(config, output_queue)

        # Check if key rules are detected via routing_manager
        logger.info(f"Key rules found: {len(ingestor.routing_manager.key_rules)}")
        for rule in ingestor.routing_manager.key_rules:
            logger.info(f"  - {rule.name}: {rule.redis_pattern}")

        # Start the ingestor
        ingestor_task = asyncio.create_task(ingestor.start())

        # Give it time to subscribe
        await asyncio.sleep(2)

        # Set test keys
        logger.info("\nSetting test keys...")
        test_keys = [
            ("Wearables:WatchDevices:test_device_001:Position", {
                "position_x": 50.0,
                "position_y": 75.0,
                "zone": "MainHall",
                "device_id": "test_device_001",
                "ticket_id": "TICKET_001"
            }),
            ("System:TotalVisitors", "100"),
            ("Weather:Temperature", "22.5")
        ]

        for key, value in test_keys:
            if isinstance(value, dict):
                await redis_client.set(key, json.dumps(value))
            else:
                await redis_client.set(key, value)
            logger.info(f"Set key: {key}")
            await asyncio.sleep(0.1)

        # Wait for processing
        await asyncio.sleep(2)

        logger.info(f"\nOutput queue size: {output_queue.qsize()}")

        # Process queued events
        events_received = []
        while not output_queue.empty():
            event = await output_queue.get()
            events_received.append(event)
            logger.info(f"  Event: {event.key_or_channel} -> {event.routing_rule.target_table}")

        # Stop the ingestor
        await ingestor.stop()
        ingestor_task.cancel()
        try:
            await ingestor_task
        except asyncio.CancelledError:
            pass

        if events_received:
            logger.info(f"\n✅ Successfully received {len(events_received)} key events")
        else:
            logger.warning("\n⚠️ No key events received")

        return len(events_received) > 0

    finally:
        await redis_client.aclose()


async def test_advanced_wildcards():
    """Test advanced wildcard patterns."""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING ADVANCED WILDCARD PATTERNS")
    logger.info("=" * 60)

    redis_client = redis.Redis.from_url("redis://localhost:6379/0")

    try:
        pubsub = redis_client.pubsub()

        # Test various wildcard patterns
        test_patterns = [
            ("*", "Match everything"),
            ("Wearables:*", "Match all Wearables"),
            ("*:BioSensors", "Match all BioSensors channels"),
            ("Wearables:*:*:*", "Match Wearables with 3 segments after"),
            ("?earables:*", "Single character wildcard"),
            ("Wearables:Watch*:*:*", "Prefix wildcard"),
        ]

        for pattern, description in test_patterns:
            await pubsub.psubscribe(pattern)
            logger.info(f"Pattern: '{pattern}' - {description}")

        # Test channels
        test_channels = [
            "Wearables:WatchDevices:001:BioSensors",
            "Wearables:SmartBand:002:HeartRate",
            "System:Status",
            "Tearables:Device:001:Sensor",  # Matches ?earables
            "Wearables:WatchPro:003:Data",
            "Sensors:Room:001:BioSensors",
        ]

        # Publish and check matches
        async def publish_test_data():
            await asyncio.sleep(0.5)
            for channel in test_channels:
                await redis_client.publish(channel, json.dumps({"test": True}))
                await asyncio.sleep(0.05)

        publish_task = asyncio.create_task(publish_test_data())

        # Collect matches
        matches = {}
        timeout_at = asyncio.get_event_loop().time() + 3.0

        while asyncio.get_event_loop().time() < timeout_at:
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1),
                    timeout=0.2
                )
                if message and message.get('type') == 'pmessage':
                    channel = message.get('channel', b'').decode('utf-8') if isinstance(message.get('channel'), bytes) else message.get('channel', '')
                    pattern = message.get('pattern', b'').decode('utf-8') if isinstance(message.get('pattern'), bytes) else message.get('pattern', '')
                    if channel and pattern:
                        if channel not in matches:
                            matches[channel] = []
                        matches[channel].append(pattern)
            except asyncio.TimeoutError:
                continue

        await publish_task

        # Display results
        logger.info("\nWildcard Matching Results:")
        for channel in test_channels:
            if channel in matches:
                logger.info(f"  ✅ '{channel}' matched by: {', '.join(matches[channel])}")
            else:
                logger.info(f"  ❌ '{channel}' - no matches")

        await pubsub.aclose()

        return len(matches) > 0

    finally:
        await redis_client.aclose()


async def main():
    """Main test function."""
    logger.info("=" * 60)
    logger.info("REDIS WILDCARD SUBSCRIPTION DEBUG TEST")
    logger.info("=" * 60)

    # Test channel subscriptions with wildcards
    channel_success = await test_channel_subscription()

    # Test key subscriptions
    key_success = await test_key_subscription()

    # Test advanced wildcard patterns
    wildcard_success = await test_advanced_wildcards()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    if channel_success and key_success and wildcard_success:
        logger.info("✅ All subscription types including wildcards are working!")
    else:
        if channel_success:
            logger.info("✅ Channel subscriptions with wildcards working")
        else:
            logger.warning("⚠️ Channel subscriptions not working")

        if key_success:
            logger.info("✅ Key subscriptions working")
        else:
            logger.warning("⚠️ Key subscriptions not working")

        if wildcard_success:
            logger.info("✅ Advanced wildcard patterns working")
        else:
            logger.warning("⚠️ Advanced wildcard patterns not working")

    if not channel_success:
        logger.info("\n🔍 CHANNEL SUBSCRIPTION ISSUES:")
        logger.info("1. Check if Redis pub/sub is enabled")
        logger.info("2. Verify the channel patterns match exactly")
        logger.info("3. Ensure the ingestor starts the channel subscriber task")
        logger.info("4. Check wildcard pattern syntax (* for multiple chars, ? for single)")

    if not key_success:
        logger.info("\n🔍 KEY SUBSCRIPTION ISSUES:")
        logger.info("1. Check if keyspace notifications are enabled (notify-keyspace-events)")
        logger.info("2. Verify the key patterns match")
        logger.info("3. Ensure the ingestor subscribes to __keyspace@0__:* channel")

    logger.info("\n📝 WILDCARD PATTERN GUIDE:")
    logger.info("  * - Matches any sequence of characters")
    logger.info("  ? - Matches any single character")
    logger.info("  Examples:")
    logger.info("    'Wearables:*' - Matches any channel starting with 'Wearables:'")
    logger.info("    '*:BioSensors' - Matches any channel ending with ':BioSensors'")
    logger.info("    'Wearables:*:*:*' - Matches Wearables with exactly 3 segments after")


if __name__ == "__main__":
    asyncio.run(main())

