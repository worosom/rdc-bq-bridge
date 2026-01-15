#!/usr/bin/env python3
"""
Redis Data Verification Script

This script verifies that the Redis data generator is working correctly
by checking the data it produces and monitoring Redis activity.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List

import redis.asyncio as redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedisDataVerifier:
    """Verifies Redis data generation and structure."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis_client = None
    
    async def connect(self) -> None:
        """Connect to Redis."""
        self.redis_client = redis.from_url(self.redis_url)
        await self.redis_client.ping()
        logger.info("Connected to Redis successfully")
    
    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Disconnected from Redis")
    
    async def verify_keyspace_notifications(self) -> bool:
        """Verify that keyspace notifications are enabled."""
        try:
            config = await self.redis_client.config_get("notify-keyspace-events")
            current_value = config.get("notify-keyspace-events", "")
            
            logger.info(f"Current keyspace notifications config: '{current_value}'")
            
            # Check if KEA (or similar) is set
            required_chars = {'K', 'E', 'A'}
            enabled_chars = set(current_value.upper())
            
            if required_chars.issubset(enabled_chars):
                logger.info("✓ Keyspace notifications are properly configured")
                return True
            else:
                logger.warning("✗ Keyspace notifications may not be properly configured")
                logger.info("To fix, run: CONFIG SET notify-keyspace-events KEA")
                return False
                
        except Exception as e:
            logger.error(f"Error checking keyspace notifications: {e}")
            return False
    
    async def list_existing_keys(self) -> Dict[str, List[str]]:
        """List all existing keys by pattern."""
        patterns = {
            "wearable_positions": "Wearables:WatchDevices:*:Position",
            "global_state": "Rooms:*",
            "system_state": "System:*",
            "weather_state": "Weather:*"
        }
        
        results = {}
        
        for category, pattern in patterns.items():
            try:
                keys = await self.redis_client.keys(pattern)
                keys = [key.decode() if isinstance(key, bytes) else key for key in keys]
                results[category] = keys
                logger.info(f"{category}: {len(keys)} keys found")
                
                # Show first few keys as examples
                if keys:
                    examples = keys[:3]
                    logger.info(f"  Examples: {examples}")
                    
            except Exception as e:
                logger.error(f"Error listing keys for {category}: {e}")
                results[category] = []
        
        return results
    
    async def sample_key_data(self, keys: List[str], max_samples: int = 3) -> None:
        """Sample data from keys to verify structure."""
        if not keys:
            return
        
        sample_keys = keys[:max_samples]
        
        for key in sample_keys:
            try:
                value = await self.redis_client.get(key)
                if value:
                    value_str = value.decode() if isinstance(value, bytes) else str(value)
                    
                    # Try to parse as JSON
                    try:
                        data = json.loads(value_str)
                        logger.info(f"Key '{key}': {json.dumps(data, indent=2)}")
                    except json.JSONDecodeError:
                        logger.info(f"Key '{key}': {value_str}")
                else:
                    logger.info(f"Key '{key}': <empty>")
                    
            except Exception as e:
                logger.error(f"Error sampling key '{key}': {e}")
    
    async def monitor_channel_activity(self, duration: int = 10) -> Dict[str, int]:
        """Monitor channel activity for biometric data."""
        channel_stats = {}
        
        try:
            pubsub = self.redis_client.pubsub()
            await pubsub.psubscribe("Wearables:WatchDevices:*:BioSensors")
            
            logger.info(f"Monitoring channel activity for {duration} seconds...")
            start_time = time.time()
            
            while time.time() - start_time < duration:
                try:
                    message = await asyncio.wait_for(pubsub.get_message(), timeout=1.0)
                    if message and message['type'] == 'pmessage':
                        channel = message['channel'].decode() if isinstance(message['channel'], bytes) else message['channel']
                        channel_stats[channel] = channel_stats.get(channel, 0) + 1
                        
                        # Log first message from each channel
                        if channel_stats[channel] == 1:
                            try:
                                data = json.loads(message['data'])
                                logger.info(f"First message from {channel}: {json.dumps(data, indent=2)}")
                            except (json.JSONDecodeError, TypeError):
                                logger.info(f"First message from {channel}: {message['data']}")
                
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error monitoring channels: {e}")
            
            await pubsub.unsubscribe()
            
        except Exception as e:
            logger.error(f"Failed to monitor channel activity: {e}")
        
        return channel_stats
    
    async def monitor_keyspace_events(self, duration: int = 10) -> Dict[str, int]:
        """Monitor keyspace events for key changes."""
        event_stats = {}
        
        try:
            pubsub = self.redis_client.pubsub()
            await pubsub.psubscribe("__keyspace@0__:*")
            
            logger.info(f"Monitoring keyspace events for {duration} seconds...")
            start_time = time.time()
            
            while time.time() - start_time < duration:
                try:
                    message = await asyncio.wait_for(pubsub.get_message(), timeout=1.0)
                    if message and message['type'] == 'pmessage':
                        # Extract key name from the channel
                        channel = message['channel'].decode() if isinstance(message['channel'], bytes) else message['channel']
                        key_name = channel.replace("__keyspace@0__:", "")
                        event_type = message['data'].decode() if isinstance(message['data'], bytes) else message['data']
                        
                        event_key = f"{event_type}:{key_name}"
                        event_stats[event_key] = event_stats.get(event_key, 0) + 1
                        
                        # Log first few events
                        if sum(event_stats.values()) <= 5:
                            logger.info(f"Keyspace event: {event_type} on key '{key_name}'")
                
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Error monitoring keyspace events: {e}")
            
            await pubsub.unsubscribe()
            
        except Exception as e:
            logger.error(f"Failed to monitor keyspace events: {e}")
        
        return event_stats
    
    async def run_comprehensive_verification(self) -> bool:
        """Run a comprehensive verification of Redis data."""
        logger.info("=" * 60)
        logger.info("Starting Redis Data Verification")
        logger.info("=" * 60)
        
        success = True
        
        try:
            # 1. Check keyspace notifications
            logger.info("Step 1: Verifying keyspace notifications...")
            if not await self.verify_keyspace_notifications():
                success = False
            
            # 2. List existing keys
            logger.info("\nStep 2: Listing existing keys...")
            key_results = await self.list_existing_keys()
            
            total_keys = sum(len(keys) for keys in key_results.values())
            if total_keys == 0:
                logger.warning("No test data found. Make sure the data generator is running.")
                success = False
            else:
                logger.info(f"Total keys found: {total_keys}")
            
            # 3. Sample key data
            logger.info("\nStep 3: Sampling key data...")
            for category, keys in key_results.items():
                if keys:
                    logger.info(f"\nSampling {category} data:")
                    await self.sample_key_data(keys)
            
            # 4. Monitor channel activity
            logger.info("\nStep 4: Monitoring channel activity...")
            channel_stats = await self.monitor_channel_activity(10)
            
            if channel_stats:
                logger.info("Channel activity detected:")
                for channel, count in channel_stats.items():
                    logger.info(f"  {channel}: {count} messages")
            else:
                logger.warning("No channel activity detected. Check if data generator is publishing.")
                success = False
            
            # 5. Monitor keyspace events
            logger.info("\nStep 5: Monitoring keyspace events...")
            event_stats = await self.monitor_keyspace_events(10)
            
            if event_stats:
                logger.info("Keyspace events detected:")
                for event, count in sorted(event_stats.items()):
                    logger.info(f"  {event}: {count} times")
            else:
                logger.warning("No keyspace events detected. Check keyspace notifications config.")
                success = False
        
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            success = False
        
        logger.info("\n" + "=" * 60)
        if success:
            logger.info("Redis Data Verification PASSED")
        else:
            logger.error("Redis Data Verification FAILED")
        logger.info("=" * 60)
        
        return success


async def main():
    """Main function for Redis data verification."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Redis Data Verification Script")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0",
                       help="Redis connection URL")
    parser.add_argument("--mode", choices=["full", "keys-only", "channels-only", "events-only"],
                       default="full", help="Verification mode")
    
    args = parser.parse_args()
    
    verifier = RedisDataVerifier(args.redis_url)
    
    try:
        await verifier.connect()
        
        if args.mode == "full":
            success = await verifier.run_comprehensive_verification()
            return 0 if success else 1
        
        elif args.mode == "keys-only":
            logger.info("Listing keys only...")
            await verifier.verify_keyspace_notifications()
            key_results = await verifier.list_existing_keys()
            for category, keys in key_results.items():
                if keys:
                    await verifier.sample_key_data(keys)
        
        elif args.mode == "channels-only":
            logger.info("Monitoring channels only...")
            stats = await verifier.monitor_channel_activity(30)
            logger.info(f"Channel statistics: {stats}")
        
        elif args.mode == "events-only":
            logger.info("Monitoring keyspace events only...")
            stats = await verifier.monitor_keyspace_events(30)
            logger.info(f"Event statistics: {stats}")
    
    except Exception as e:
        logger.error(f"Verification error: {e}")
        return 1
    finally:
        await verifier.disconnect()
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))

