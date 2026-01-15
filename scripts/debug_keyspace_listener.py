#!/usr/bin/env python3
"""
Debug script to test Redis keyspace event notifications and listeners.
Tests various keyspace event types and pattern matching.
"""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Dict, List, Set, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis.asyncio as redis
from src.config import load_config, get_default_config_path
from src.redis_ingestor import RedisIngestor
from src.redis_listener import KeyspaceListener

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KeyspaceDebugger:
    """Helper class for debugging keyspace notifications."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis_client = None
        self.events_received: List[Dict[str, Any]] = []

    async def connect(self):
        """Connect to Redis."""
        self.redis_client = redis.Redis.from_url(self.redis_url)
        logger.info(f"Connected to Redis at {self.redis_url}")

    async def disconnect(self):
        """Disconnect from Redis."""
        if self.redis_client:
            await self.redis_client.aclose()

    async def check_notification_config(self) -> Dict[str, str]:
        """Check current keyspace notification configuration."""
        config = await self.redis_client.config_get("notify-keyspace-events")
        current_value = config.get("notify-keyspace-events", "")

        logger.info("=" * 60)
        logger.info("KEYSPACE NOTIFICATION CONFIGURATION")
        logger.info("=" * 60)
        logger.info(f"Current value: '{current_value}'")

        # Decode the configuration
        flags = {
            'K': 'Keyspace events (operations on keys)',
            'E': 'Keyevent events (keys affected by operations)',
            'g': 'Generic commands (DEL, EXPIRE, RENAME, ...)',
            'x': 'Expired events',
            'e': 'Evicted events',
            's': 'Set commands',
            'h': 'Hash commands',
            'z': 'Sorted set commands',
            'l': 'List commands',
            't': 'Stream commands',
            'A': 'Alias for g$lshzxet (all events)',
        }

        if current_value:
            logger.info("\nEnabled notifications:")
            for flag, description in flags.items():
                if flag in current_value:
                    logger.info(f"  [{flag}] {description}")
        else:
            logger.warning("⚠️ No keyspace notifications enabled!")

        return {"current": current_value, "flags": flags}

    async def enable_all_notifications(self):
        """Enable all keyspace notifications."""
        await self.redis_client.config_set("notify-keyspace-events", "KEA")
        logger.info("✅ Enabled all keyspace notifications (KEA)")

    async def test_basic_keyspace_events(self):
        """Test basic keyspace event notifications."""
        logger.info("\n" + "=" * 60)
        logger.info("TESTING BASIC KEYSPACE EVENTS")
        logger.info("=" * 60)

        pubsub = self.redis_client.pubsub()

        # Subscribe to keyspace notifications for database 0
        patterns = [
            "__keyspace@0__:*",  # All keyspace events
            "__keyevent@0__:*",  # All keyevent events
        ]

        for pattern in patterns:
            await pubsub.psubscribe(pattern)
            logger.info(f"Subscribed to pattern: {pattern}")

        # Perform various operations
        test_operations = [
            ("SET", "test:key1", "value1"),
            ("HSET", "test:hash1", {"field1": "value1", "field2": "value2"}),
            ("LPUSH", "test:list1", "item1"),
            ("ZADD", "test:zset1", {"member1": 1.0}),
            ("DEL", "test:key1", None),
            ("EXPIRE", "test:hash1", 10),
        ]

        # Start collecting events
        events = []

        async def collect_events():
            timeout_at = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < timeout_at:
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1),
                        timeout=0.2
                    )
                    if message and message.get('type') == 'pmessage':
                        event = self._parse_event(message)
                        if event:
                            events.append(event)
                            logger.info(f"📨 Event: {event['type']} on {event['key']} - {event['operation']}")
                except asyncio.TimeoutError:
                    continue

        # Start event collection
        collect_task = asyncio.create_task(collect_events())

        # Wait a bit for subscription to be ready
        await asyncio.sleep(0.5)

        # Execute operations
        logger.info("\nExecuting test operations:")
        for op_type, key, value in test_operations:
            if op_type == "SET":
                await self.redis_client.set(key, value)
                logger.info(f"  SET {key} = {value}")
            elif op_type == "HSET":
                for field, val in value.items():
                    await self.redis_client.hset(key, field, val)
                logger.info(f"  HSET {key} {value}")
            elif op_type == "LPUSH":
                await self.redis_client.lpush(key, value)
                logger.info(f"  LPUSH {key} {value}")
            elif op_type == "ZADD":
                for member, score in value.items():
                    await self.redis_client.zadd(key, {member: score})
                logger.info(f"  ZADD {key} {value}")
            elif op_type == "DEL":
                await self.redis_client.delete(key)
                logger.info(f"  DEL {key}")
            elif op_type == "EXPIRE":
                await self.redis_client.expire(key, value)
                logger.info(f"  EXPIRE {key} {value}s")
            await asyncio.sleep(0.1)

        # Wait for events
        await collect_task

        # Analyze results
        logger.info(f"\n📊 Received {len(events)} events")

        # Group events by type
        keyspace_events = [e for e in events if e['type'] == 'keyspace']
        keyevent_events = [e for e in events if e['type'] == 'keyevent']

        logger.info(f"  Keyspace events: {len(keyspace_events)}")
        logger.info(f"  Keyevent events: {len(keyevent_events)}")

        await pubsub.aclose()

        return len(events) > 0

    async def test_pattern_matching(self):
        """Test pattern matching for specific keys."""
        logger.info("\n" + "=" * 60)
        logger.info("TESTING PATTERN MATCHING")
        logger.info("=" * 60)

        pubsub = self.redis_client.pubsub()

        # Subscribe to specific key patterns
        patterns = [
            "__keyspace@0__:Wearables:*",
            "__keyspace@0__:System:*",
            "__keyspace@0__:Weather:*",
            "__keyevent@0__:set",
            "__keyevent@0__:del",
            "__keyevent@0__:expire",
        ]

        for pattern in patterns:
            await pubsub.psubscribe(pattern)
            logger.info(f"Subscribed to: {pattern}")

        # Test keys that should and shouldn't match
        test_keys = [
            ("Wearables:WatchDevices:001:Position", {"x": 10, "y": 20}, True),
            ("Wearables:SmartBand:002:HeartRate", {"rate": 75}, True),
            ("System:TotalVisitors", "150", True),
            ("System:ActiveDevices", "25", True),
            ("Weather:Temperature", "23.5", True),
            ("Weather:Humidity", "65", True),
            ("Other:Random:Key", "value", False),  # Should not match
            ("RandomKey", "value", False),  # Should not match
        ]

        events = []

        async def collect_events():
            timeout_at = asyncio.get_event_loop().time() + 5.0
            while asyncio.get_event_loop().time() < timeout_at:
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1),
                        timeout=0.2
                    )
                    if message and message.get('type') == 'pmessage':
                        event = self._parse_event(message)
                        if event:
                            events.append(event)
                            logger.info(f"  ✅ Matched: {event['key']} ({event['operation']})")
                except asyncio.TimeoutError:
                    continue

        # Start collection
        collect_task = asyncio.create_task(collect_events())

        # Wait for subscription
        await asyncio.sleep(0.5)

        # Set test keys
        logger.info("\nSetting test keys:")
        for key, value, should_match in test_keys:
            if isinstance(value, dict):
                await self.redis_client.set(key, json.dumps(value))
            else:
                await self.redis_client.set(key, value)
            logger.info(f"  SET {key} (expect match: {should_match})")
            await asyncio.sleep(0.1)

        # Wait for events
        await collect_task

        # Verify matches
        matched_keys = {e['key'] for e in events if e['type'] == 'keyspace'}
        expected_matches = {k for k, _, should_match in test_keys if should_match}
        unexpected_matches = {k for k, _, should_match in test_keys if not should_match}

        logger.info(f"\n📊 Pattern Matching Results:")
        logger.info(f"  Total events: {len(events)}")
        logger.info(f"  Matched keys: {len(matched_keys)}")

        correct_matches = matched_keys & expected_matches
        missed_matches = expected_matches - matched_keys
        wrong_matches = matched_keys & unexpected_matches

        if correct_matches:
            logger.info(f"  ✅ Correctly matched: {len(correct_matches)}")
        if missed_matches:
            logger.warning(f"  ⚠️ Missed: {missed_matches}")
        if wrong_matches:
            logger.warning(f"  ❌ Incorrectly matched: {wrong_matches}")

        await pubsub.aclose()

        return len(missed_matches) == 0 and len(wrong_matches) == 0

    async def test_expiration_events(self):
        """Test key expiration events."""
        logger.info("\n" + "=" * 60)
        logger.info("TESTING EXPIRATION EVENTS")
        logger.info("=" * 60)

        pubsub = self.redis_client.pubsub()

        # Subscribe to expiration events
        patterns = [
            "__keyevent@0__:expired",
            "__keyspace@0__:*",
        ]

        for pattern in patterns:
            await pubsub.psubscribe(pattern)
            logger.info(f"Subscribed to: {pattern}")

        events = []

        async def collect_events():
            timeout_at = asyncio.get_event_loop().time() + 10.0
            while asyncio.get_event_loop().time() < timeout_at:
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1),
                        timeout=0.2
                    )
                    if message and message.get('type') == 'pmessage':
                        event = self._parse_event(message)
                        if event and 'expire' in event['operation']:
                            events.append(event)
                            logger.info(f"  ⏰ Expiration event: {event['key']}")
                except asyncio.TimeoutError:
                    continue

        # Start collection
        collect_task = asyncio.create_task(collect_events())

        # Wait for subscription
        await asyncio.sleep(0.5)

        # Set keys with short TTL
        logger.info("\nSetting keys with TTL:")
        test_keys = [
            ("expire:test1", "value1", 2),
            ("expire:test2", "value2", 3),
            ("expire:test3", "value3", 4),
        ]

        for key, value, ttl in test_keys:
            await self.redis_client.set(key, value, ex=ttl)
            logger.info(f"  SET {key} with TTL={ttl}s")

        # Wait for expirations
        logger.info("\nWaiting for expirations...")
        await collect_task

        logger.info(f"\n📊 Expiration Results:")
        logger.info(f"  Expected expirations: {len(test_keys)}")
        logger.info(f"  Received expiration events: {len(events)}")

        if len(events) == len(test_keys):
            logger.info("  ✅ All expiration events received")
        else:
            logger.warning(f"  ⚠️ Missing {len(test_keys) - len(events)} expiration events")

        await pubsub.aclose()

        return len(events) == len(test_keys)

    async def test_with_ingestor(self):
        """Test keyspace events with RedisIngestor."""
        logger.info("\n" + "=" * 60)
        logger.info("TESTING WITH REDIS INGESTOR")
        logger.info("=" * 60)

        try:
            config = load_config(get_default_config_path())
            output_queue = asyncio.Queue()

            ingestor = RedisIngestor(config, output_queue)

            # Check configuration
            logger.info(f"Key rules configured: {len(ingestor.routing_manager.key_rules)}")
            for rule in ingestor.routing_manager.key_rules:
                logger.info(f"  - {rule.name}: {rule.redis_pattern}")

            # Start ingestor
            ingestor_task = asyncio.create_task(ingestor.start())

            # Wait for startup
            await asyncio.sleep(2)

            # Test various key operations
            logger.info("\nTesting key operations:")

            test_operations = [
                ("SET", "Wearables:WatchDevices:test001:Position", {
                    "position_x": 100,
                    "position_y": 200,
                    "zone": "TestZone",
                    "timestamp": datetime.now(UTC).isoformat()
                }),
                ("SET", "System:TotalVisitors", "250"),
                ("HSET", "Wearables:WatchDevices:test002:Status", {
                    "battery": 85,
                    "connected": True
                }),
                ("DEL", "Wearables:WatchDevices:test001:Position", None),
            ]

            for op_type, key, value in test_operations:
                if op_type == "SET":
                    if isinstance(value, dict):
                        await self.redis_client.set(key, json.dumps(value))
                    else:
                        await self.redis_client.set(key, value)
                    logger.info(f"  SET {key}")
                elif op_type == "HSET" and value:
                    for field, val in value.items():
                        await self.redis_client.hset(key, field, json.dumps(val) if not isinstance(val, str) else val)
                    logger.info(f"  HSET {key}")
                elif op_type == "DEL":
                    await self.redis_client.delete(key)
                    logger.info(f"  DEL {key}")
                await asyncio.sleep(0.5)

            # Wait for processing
            await asyncio.sleep(2)

            # Check queue
            logger.info(f"\n📊 Ingestor Results:")
            logger.info(f"  Output queue size: {output_queue.qsize()}")

            events_received = []
            while not output_queue.empty():
                event = await output_queue.get()
                events_received.append(event)
                logger.info(f"    Event: {event.key_or_channel} -> {event.routing_rule.target_table}")

            # Stop ingestor
            await ingestor.stop()
            ingestor_task.cancel()
            try:
                await ingestor_task
            except asyncio.CancelledError:
                pass

            if events_received:
                logger.info(f"  ✅ Received {len(events_received)} events through ingestor")
                return True
            else:
                logger.warning("  ⚠️ No events received through ingestor")
                return False

        except Exception as e:
            logger.error(f"Error testing with ingestor: {e}")
            return False

    def _parse_event(self, message: Dict) -> Dict[str, Any]:
        """Parse a keyspace/keyevent message."""
        try:
            pattern = message.get('pattern', b'').decode('utf-8') if isinstance(message.get('pattern'), bytes) else message.get('pattern', '')
            channel = message.get('channel', b'').decode('utf-8') if isinstance(message.get('channel'), bytes) else message.get('channel', '')
            data = message.get('data', b'').decode('utf-8') if isinstance(message.get('data'), bytes) else message.get('data', '')

            # Determine event type and extract key/operation
            if '__keyspace@' in pattern:
                # Keyspace event: channel has the key, data has the operation
                event_type = 'keyspace'
                key = channel.split(':', 1)[1] if ':' in channel else channel
                operation = data
            elif '__keyevent@' in pattern:
                # Keyevent event: channel has the operation, data has the key
                event_type = 'keyevent'
                operation = channel.split(':', 1)[1] if ':' in channel else channel
                key = data
            else:
                return None

            return {
                'type': event_type,
                'key': key,
                'operation': operation,
                'pattern': pattern,
                'timestamp': datetime.now(UTC).isoformat()
            }
        except Exception as e:
            logger.error(f"Error parsing event: {e}")
            return None


async def main():
    """Main test function."""
    debugger = KeyspaceDebugger()

    try:
        await debugger.connect()

        # Check current configuration
        config = await debugger.check_notification_config()

        # Enable notifications if needed
        if not config['current'] or 'K' not in config['current'] or 'E' not in config['current']:
            logger.info("\n⚠️ Keyspace notifications not fully enabled, enabling now...")
            await debugger.enable_all_notifications()

        # Run tests
        results = {
            "basic_events": await debugger.test_basic_keyspace_events(),
            "pattern_matching": await debugger.test_pattern_matching(),
            "expiration_events": await debugger.test_expiration_events(),
            "ingestor_integration": await debugger.test_with_ingestor(),
        }

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)

        all_passed = all(results.values())

        for test_name, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            logger.info(f"{test_name.replace('_', ' ').title()}: {status}")

        if all_passed:
            logger.info("\n🎉 All keyspace listener tests passed!")
        else:
            logger.warning("\n⚠️ Some tests failed. Review the output above for details.")

            # Provide troubleshooting tips
            logger.info("\n🔍 TROUBLESHOOTING TIPS:")

            if not results["basic_events"]:
                logger.info("\nBasic Events Issues:")
                logger.info("  1. Ensure Redis keyspace notifications are enabled (notify-keyspace-events KEA)")
                logger.info("  2. Check Redis version (>= 2.8.0 required)")
                logger.info("  3. Verify subscription patterns match database number")

            if not results["pattern_matching"]:
                logger.info("\nPattern Matching Issues:")
                logger.info("  1. Check pattern syntax - use * for wildcards")
                logger.info("  2. Verify key prefixes match exactly")
                logger.info("  3. Remember patterns are case-sensitive")

            if not results["expiration_events"]:
                logger.info("\nExpiration Events Issues:")
                logger.info("  1. Ensure 'x' flag is in notify-keyspace-events")
                logger.info("  2. Check Redis server time and TTL accuracy")
                logger.info("  3. Note: Redis checks expiration lazily")

            if not results["ingestor_integration"]:
                logger.info("\nIngestor Integration Issues:")
                logger.info("  1. Check config.yaml for correct key_rules")
                logger.info("  2. Verify RedisIngestor starts KeyspaceListener")
                logger.info("  3. Check routing rules match key patterns")

    finally:
        await debugger.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

