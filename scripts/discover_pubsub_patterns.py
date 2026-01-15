#!/usr/bin/env python3
"""
Redis Pub/Sub Pattern Discovery Utility

This script connects to Redis and listens to ALL pub/sub patterns using the "*" wildcard.
It collects all unique channel patterns that occur and displays them in real-time.

Usage:
    python scripts/discover_pubsub_patterns.py
"""

import logging
import sys
from typing import Set

import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("PubSubDiscovery")

# Patterns to filter out (channels starting with these prefixes will be ignored)
FILTER_PATTERNS: list[str] = [
#     "__keyspace@",
    "__keyevent@",
    "jobcenter"
]

def main():
    # Redis connection details (hardcoded from config.yaml)
    REDIS_HOST = "10.23.104.21"
    REDIS_PORT = 6379
    REDIS_USER = "default"
    REDIS_PASSWORD = "DADmin#100"
    
    # Track discovered patterns
    discovered_patterns: Set[str] = set()
    message_count = 0
    filtered_count = 0  # Count of filtered messages
    
    print("\n" + "=" * 70)
    print("Redis Pub/Sub Pattern Discovery")
    print("=" * 70)
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    
    # Connect to Redis
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            username=REDIS_USER,
            password=REDIS_PASSWORD,
            decode_responses=False,  # Keep as bytes to handle arbitrary data
            socket_connect_timeout=5
        )
        r.ping()
        logger.info(f"✓ Connected to Redis successfully")
    except Exception as e:
        logger.error(f"✗ Failed to connect to Redis: {e}")
        sys.exit(1)
    
    # Subscribe to all patterns
    pubsub = r.pubsub()
    pubsub.psubscribe("*")
    logger.info("✓ Subscribed to pattern: *")
    
    print("\n" + "=" * 70)
    print("Listening for pub/sub messages... (Ctrl+C to stop)")
    print(f"Filtering out patterns containing: {', '.join(FILTER_PATTERNS)}")
    print("=" * 70)
    print()
    
    try:
        for message in pubsub.listen():
            if message['type'] == 'pmessage':
                channel = message['channel'].decode('utf-8', errors='replace')
                
                # Filter out patterns based on FILTER_PATTERNS list
                if any(pattern in channel for pattern in FILTER_PATTERNS):
                    filtered_count += 1
                    continue
                
                message_count += 1
                
                # Add to discovered patterns set
                if channel not in discovered_patterns:
                    discovered_patterns.add(channel)
                    print(f"[NEW PATTERN #{len(discovered_patterns)}] {channel}")
                
                # Show a dot for each message to indicate activity
                if message_count % 10 == 0:
                    print(f"  ... {message_count} messages received, {filtered_count} filtered", end='\r')
    
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("Summary")
        print("=" * 70)
        print(f"Total messages received: {message_count}")
        print(f"Filtered messages ({', '.join(FILTER_PATTERNS)}): {filtered_count}")
        print(f"Unique patterns discovered: {len(discovered_patterns)}")
        print()
        
        if discovered_patterns:
            print("Discovered Patterns:")
            print("-" * 70)
            for pattern in sorted(discovered_patterns):
                print(f"  • {pattern}")
        else:
            print("No patterns were discovered. Possible reasons:")
            print("  • No active pub/sub activity on this Redis instance")
            print("  • Publishers might be using different Redis database")
            print("  • Check if applications are actively publishing data")
        
        print("=" * 70)
        
    except Exception as e:
        logger.error(f"Error during monitoring: {e}")
    finally:
        pubsub.close()
        r.close()

if __name__ == "__main__":
    main()
