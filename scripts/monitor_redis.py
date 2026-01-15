#!/usr/bin/env python3
"""
Redis Activity Monitor

This script connects to the configured Redis instance and monitors all activity
related to the configured routing rules. It subscribes to:
1. All Pub/Sub channels defined in the config.
2. Keyspace notifications for all Key patterns defined in the config.

Usage:
    python scripts/monitor_redis.py
"""

import logging
import sys
import time
from typing import List, Set

import msgpack
import redis
from redis.cache import CacheConfig
import yaml

# Add project root to path
sys.path.append(".")

try:
    from src.config import load_config, get_default_config_path
except ImportError:
    print("Error: Could not import src.config. Run this script from the project root.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("RedisMonitor")

# Patterns to exclude from monitoring
EXCLUDE_PATTERNS = [
#     "__key",
#     "{jobc",
#     "Wearables:Scent:RegisterDevic",
]

INCLUDE_PATTERNS = [
#     "Visitors:*:EmpaticaDeviceID"
    "*"
]

def main():
    # Load configuration
    try:
        config_path = get_default_config_path()
        config = load_config(config_path)
        logger.info(f"Loaded configuration from {config_path}")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Collect patterns to subscribe to
    channel_patterns: Set[str] = set()
    keyspace_patterns: Set[str] = set()

    print("\n=== Monitoring Targets ===")
    for rule in config.pipeline.routing_rules:
        if rule.redis_source_type == "channel":
            channel_patterns.add(rule.redis_pattern)
            print(f"  [Channel] {rule.name}: {rule.redis_pattern}")
        elif rule.redis_source_type == "key":
            # Convert key pattern to keyspace pattern
            # __keyspace@<db>__:<pattern>
            # We'll use wildcard for db: __keyspace@*__:<pattern>
            pattern = f"__keyspace@*__:{rule.redis_pattern}"
            keyspace_patterns.add(pattern)
            print(f"  [Keyspace] {rule.name}: {rule.redis_pattern}")

    if not channel_patterns and not keyspace_patterns:
        logger.warning("No routing rules found in config!")
        sys.exit(0)

    # Connect to Redis
    try:
        r = redis.Redis(
            host=config.redis.host,
            port=config.redis.port,
            username=config.redis.username,
            password=config.redis.password,
            decode_responses=False,  # Keep as bytes to handle arbitrary data
            socket_connect_timeout=5,
        )
        r.ping()
        logger.info(f"Connected to Redis at {config.redis.host}:{config.redis.port}")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)

    # Enable keyspace notifications if needed
    if keyspace_patterns:
        try:
            current_config = r.config_get("notify-keyspace-events")
            current_val = current_config.get("notify-keyspace-events", "")
            if "K" not in current_val or "E" not in current_val:
                logger.warning(f"Warning: Redis 'notify-keyspace-events' is currently '{current_val}'.")
                logger.warning("To see keyspace events, it should likely include 'KEA'.")
                # We don't automatically set it to avoid changing server state unexpectedly,
                # but we warn the user.
        except Exception as e:
            logger.warning(f"Could not check keyspace config: {e}")

    # Subscribe
    pubsub = r.pubsub()
    
    all_patterns = list(channel_patterns | keyspace_patterns) + INCLUDE_PATTERNS
    # all_patterns = INCLUDE_PATTERNS
    if all_patterns:
        pubsub.psubscribe(*all_patterns)
        logger.info(f"Subscribed to {len(all_patterns)} patterns.")

    print("\n=== Waiting for Events (Ctrl+C to stop) ===\n")

    try:
        for message in pubsub.listen():
            if message['type'] == 'pmessage':
                channel = message['channel'].decode('utf-8', errors='replace')
                data = message['data']
                
                # Filter out excluded patterns
                if any(channel.startswith(pattern) for pattern in EXCLUDE_PATTERNS):
                    continue
                
                # Format output based on source
                if channel.startswith('__keyspace'):
                    # It's a keyspace notification
                    # channel looks like: __keyspace@0__:Visitors:123:Status
                    # data is the operation (set, hset, etc.)
                    operation = data.decode('utf-8', errors='replace')
                    key = channel.split(':', 1)[1] if ':' in channel else channel
                    
                    # Fetch the current value of the key
                    try:
                        value = r.get(key)
                        if value is not None:
                            # Try to decode the value
                            try:
                                decoded_value = msgpack.unpackb(value, raw=False)
                                value_str = str(decoded_value)
                            except:
                                try:
                                    value_str = value.decode('utf-8', errors='replace')
                                except:
                                    value_str = repr(value)
                            
                            # Truncate if too long
                            if len(value_str) > 200:
                                value_str = value_str[:200] + "..."
                            
                            print(f"[KEY-EVENT] {key} -> {operation} | value: {value_str}")
                        else:
                            print(f"[KEY-EVENT] {key} -> {operation} | value: None")
                    except Exception as e:
                        print(f"[KEY-EVENT] {key} -> {operation} | (could not fetch value: {e})")
                    
                else:
                    # It's a regular channel message
                    decoded_data = None
                    
                    # Try msgpack first
                    try:
                        unpacked = msgpack.unpackb(data, raw=False)
                        decoded_data = str(unpacked)
                    except:
                        pass
                    
                    # If msgpack failed, try utf-8
                    if decoded_data is None:
                        try:
                            decoded_data = data.decode('utf-8')
                        except:
                            # Fallback to repr for binary
                            decoded_data = repr(data)
                        
                    # Truncate if too long
                    if len(decoded_data) > 500:
                        decoded_data = decoded_data[:500] + "..."
                        
                    print(f"[CHANNEL] {channel} -> {decoded_data}")

    except KeyboardInterrupt:
        print("\nStopping monitor...")
    except Exception as e:
        logger.error(f"Error during monitoring: {e}")
    finally:
        pubsub.close()
        r.close()

if __name__ == "__main__":
    main()