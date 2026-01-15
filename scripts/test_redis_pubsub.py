#!/usr/bin/env python3
"""
Minimal test to verify Redis channel pub/sub is working.
"""

import asyncio
import json
import logging
from datetime import datetime

import redis.asyncio as redis

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def test_direct_pubsub():
    """Test Redis pub/sub directly without any pipeline code."""
    
    # Create two Redis connections - one for pub, one for sub
    pub_client = redis.Redis.from_url("redis://localhost:6379/0")
    sub_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    try:
        # Create pubsub object
        pubsub = sub_client.pubsub()
        
        # Subscribe to the exact pattern used in config
        pattern = "Wearables:WatchDevices:*:BioSensors"
        await pubsub.psubscribe(pattern)
        logger.info(f"Subscribed to pattern: {pattern}")
        
        # Wait a moment for subscription to be ready
        await asyncio.sleep(0.5)
        
        # Publish test messages
        test_channels = [
            "Wearables:WatchDevices:device_001:BioSensors",
            "Wearables:WatchDevices:device_002:BioSensors",
            "Wearables:WatchDevices:test_device:BioSensors"
        ]
        
        for channel in test_channels:
            data = {
                "heart_rate": 75,
                "skin_conductivity": 0.8,
                "skin_temperature": 36.5,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Publish message
            num_subscribers = await pub_client.publish(channel, json.dumps(data))
            logger.info(f"Published to {channel} - {num_subscribers} subscribers received it")
        
        # Try to receive messages
        logger.info("\nWaiting for messages...")
        messages_received = []
        
        # Use get_message with timeout
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 3:  # Wait up to 3 seconds
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message:
                messages_received.append(message)
                logger.info(f"Received message: type={message['type']}, channel={message.get('channel', b'').decode()}")
            await asyncio.sleep(0.1)
        
        # Check results
        if messages_received:
            logger.info(f"\n✅ SUCCESS: Received {len(messages_received)} messages")
            for msg in messages_received:
                channel = msg.get('channel', b'').decode()
                logger.info(f"  - Channel: {channel}")
        else:
            logger.error("\n❌ FAILURE: No messages received")
            logger.info("Possible issues:")
            logger.info("1. Redis pub/sub might be disabled")
            logger.info("2. Network/firewall issues")
            logger.info("3. Redis server configuration")
        
        await pubsub.close()
        
    finally:
        await pub_client.close()
        await sub_client.close()


async def test_with_multiple_subscribers():
    """Test with multiple subscribers to ensure messages are being published."""
    
    pub_client = redis.Redis.from_url("redis://localhost:6379/0")
    
    # Create multiple subscribers
    subscribers = []
    for i in range(3):
        sub_client = redis.Redis.from_url("redis://localhost:6379/0")
        pubsub = sub_client.pubsub()
        await pubsub.psubscribe("Wearables:WatchDevices:*:BioSensors")
        subscribers.append((sub_client, pubsub))
    
    logger.info(f"Created {len(subscribers)} subscribers")
    
    # Wait for subscriptions to be ready
    await asyncio.sleep(1)
    
    # Publish a message
    channel = "Wearables:WatchDevices:test_multi:BioSensors"
    data = {"test": "data", "timestamp": datetime.utcnow().isoformat()}
    
    num_subscribers = await pub_client.publish(channel, json.dumps(data))
    logger.info(f"\nPublished to {channel}")
    logger.info(f"Number of subscribers that received it: {num_subscribers}")
    
    if num_subscribers == 0:
        logger.error("❌ No subscribers received the message!")
        logger.info("This means the subscription pattern isn't matching or subscribers aren't ready")
    else:
        logger.info(f"✅ Message delivered to {num_subscribers} subscribers")
    
    # Clean up
    for sub_client, pubsub in subscribers:
        await pubsub.close()
        await sub_client.close()
    await pub_client.close()


async def main():
    logger.info("=" * 60)
    logger.info("REDIS PUB/SUB DIRECT TEST")
    logger.info("=" * 60)
    
    logger.info("\nTest 1: Basic pub/sub test")
    await test_direct_pubsub()
    
    logger.info("\n" + "=" * 60)
    logger.info("\nTest 2: Multiple subscribers test")
    await test_with_multiple_subscribers()
    
    logger.info("\n" + "=" * 60)
    logger.info("CONCLUSION")
    logger.info("=" * 60)
    logger.info("If both tests pass, Redis pub/sub is working correctly.")
    logger.info("If they fail, the issue is with Redis configuration, not the pipeline code.")


if __name__ == "__main__":
    asyncio.run(main())

