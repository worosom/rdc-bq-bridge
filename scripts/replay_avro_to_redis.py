#!/usr/bin/env python3
"""
Script to replay Avro-encoded Redis events back to a Redis server.
Reads events from an Avro file and executes corresponding Redis commands.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Any, cast

import fastavro
import msgpack
import redis


def replay_events(
    avro_file: Path,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    redis_password: str | None = None,
    dry_run: bool = False,
    realtime: bool = True,
    speed: float = 1.0,
    verbose: bool = False,
) -> None:
    """
    Read events from Avro file and replay them to Redis.
    
    Args:
        avro_file: Path to the .avro file
        redis_host: Redis server hostname
        redis_port: Redis server port
        redis_db: Redis database number
        redis_password: Redis password (optional)
        dry_run: If True, print commands without executing them
        realtime: If True, replay events with original timing (default: True)
        speed: Playback speed multiplier (1.0 = realtime, 2.0 = 2x speed, 0.5 = half speed)
        verbose: If True, print commands to console (default: False)
    """
    # Connect to Redis (unless dry run)
    r: redis.Redis | None  # type: ignore[type-arg]
    if not dry_run:
        r = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=False
        )
        try:
            r.ping()
            auth_info = " (authenticated)" if redis_password else ""
            print(f"✓ Connected to Redis at {redis_host}:{redis_port} (db={redis_db}){auth_info}")
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"DRY RUN MODE - commands will be printed but not executed")
        r = None
    
    # Read and replay events
    event_count = 0
    channel_count = 0
    key_count = 0
    
    first_timestamp: int | None = None
    start_time: float | None = None
    
    with open(avro_file, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            # Cast to dict to satisfy type checker
            rec = cast(dict[str, Any], record)
            
            event_type: str = rec['type']
            key: str = rec['key']
            value: bytes = rec['value']
            ttl: int | None = rec['ttl']
            timestamp: int = rec['timestamp']  # milliseconds since epoch
            
            # Handle timing for realtime playback
            if realtime:
                if first_timestamp is None:
                    # First event - initialize timing
                    first_timestamp = timestamp
                    start_time = time.time()
                else:
                    # Calculate when this event should be played relative to first event
                    assert start_time is not None  # Type narrowing
                    elapsed_ms = timestamp - first_timestamp
                    elapsed_s = elapsed_ms / 1000.0
                    adjusted_elapsed_s = elapsed_s / speed
                    
                    # Calculate target time and sleep if needed
                    target_time = start_time + adjusted_elapsed_s
                    current_time = time.time()
                    sleep_time = target_time - current_time
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    elif sleep_time < -1.0:
                        # We're falling behind by more than 1 second
                        print(f"\n⚠ Warning: Playback is {-sleep_time:.2f}s behind schedule", file=sys.stderr)
            
            if event_type == 'channel':
                # Publish to channel
                if dry_run or verbose:
                    try:
                        decoded_value = msgpack.unpackb(value, raw=False)
                        print(f"PUBLISH {key} {decoded_value}")
                    except Exception:
                        print(f"PUBLISH {key} <{len(value)} bytes (decode failed)>")
                if not dry_run:
                    assert r is not None  # Type narrowing for mypy
                    r.publish(key, value)
                channel_count += 1
                
            elif event_type == 'key':
                # Set key with optional TTL
                if dry_run or verbose:
                    try:
                        decoded_value = msgpack.unpackb(value, raw=False)
                        ttl_info = f" EX {ttl}" if ttl is not None else ""
                        print(f"SET {key}{ttl_info} {decoded_value}")
                    except Exception:
                        ttl_info = f" (TTL: {ttl}s)" if ttl is not None else ""
                        print(f"SET {key} <{len(value)} bytes (decode failed)>{ttl_info}")
                if not dry_run:
                    assert r is not None  # Type narrowing for mypy
                    if ttl is not None and ttl > 0:
                        r.setex(key, ttl, value)
                    else:
                        r.set(key, value)
                key_count += 1
            
            event_count += 1
            
            if event_count % 100 == 0:
                print(f"Processed {event_count} events...", end='\r')
    
    print(f"\n✓ Replayed {event_count} events:")
    print(f"  - {channel_count} channel publishes")
    print(f"  - {key_count} key sets")


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Replay Avro-encoded Redis events to a Redis server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Replay with original timing preserved (default)
  %(prog)s combined.avro
  
  # Replay at 2x speed with timing preserved
  %(prog)s combined.avro --speed 2.0
  
  # Replay as fast as possible (no timing delays)
  %(prog)s combined.avro --no-timing
  
  # Replay to remote Redis with authentication
  %(prog)s combined.avro --host redis.example.com --port 6380 --password mypassword
  
  # Dry run to see what would be executed
  %(prog)s combined.avro --dry-run
        """
    )
    
    parser.add_argument('avro_file', type=Path, help='Path to the .avro file')
    parser.add_argument('--host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--db', type=int, default=0, help='Redis database number (default: 0)')
    parser.add_argument('--password', type=str, default=None, help='Redis password (optional)')
    parser.add_argument('--dry-run', action='store_true', help='Print commands without executing')
    parser.add_argument('--no-timing', action='store_true', help='Replay as fast as possible without timing delays')
    parser.add_argument('--speed', type=float, default=1.0, help='Playback speed multiplier (default: 1.0)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Print Redis commands to console')
    
    args = parser.parse_args()
    
    if not args.avro_file.exists():
        print(f"✗ File not found: {args.avro_file}", file=sys.stderr)
        sys.exit(1)
    
    # Realtime is on by default, unless --no-timing is specified
    realtime = not args.no_timing
    
    if args.speed != 1.0 and args.no_timing:
        print("⚠ Warning: --speed has no effect with --no-timing", file=sys.stderr)
    
    replay_events(
        args.avro_file,
        args.host,
        args.port,
        args.db,
        args.password,
        args.dry_run,
        realtime,
        args.speed,
        args.verbose
    )


if __name__ == '__main__':
    main()