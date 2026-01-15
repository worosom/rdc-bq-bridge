#!/usr/bin/env python3
"""
Test Runner for RDC-BigQuery Bridge

This script helps run integration tests by:
1. Starting the Redis data generator
2. Running the RDC-BigQuery bridge
3. Monitoring the data flow
"""

import asyncio
import logging
import subprocess
import sys
import time
from pathlib import Path

import redis.asyncio as redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TestRunner:
    """Orchestrates testing of the RDC-BigQuery bridge."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis_client = None
        self.generator_process = None
        self.bridge_process = None
    
    async def setup_redis(self) -> bool:
        """Setup Redis for testing."""
        try:
            self.redis_client = redis.from_url(self.redis_url)
            await self.redis_client.ping()
            
            # Enable keyspace notifications if not already enabled
            await self.redis_client.config_set("notify-keyspace-events", "KEA")
            logger.info("Redis setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Redis: {e}")
            return False
    
    async def cleanup_redis(self) -> None:
        """Clean up Redis test data."""
        try:
            # Clean up test keys
            keys_to_delete = []
            
            # Get all wearable device keys
            pattern_keys = [
                "Wearables:WatchDevices:*",
                "Rooms:*",
                "System:*",
                "Weather:*"
            ]
            
            for pattern in pattern_keys:
                keys = await self.redis_client.keys(pattern)
                keys_to_delete.extend(keys)
            
            if keys_to_delete:
                await self.redis_client.delete(*keys_to_delete)
                logger.info(f"Cleaned up {len(keys_to_delete)} Redis keys")
            
        except Exception as e:
            logger.error(f"Error cleaning up Redis: {e}")
        finally:
            if self.redis_client:
                await self.redis_client.close()
    
    def start_data_generator(self, mode: str = "continuous", duration: int = 60, devices: int = 10) -> bool:
        """Start the Redis data generator."""
        try:
            script_path = Path(__file__).parent / "redis_data_generator.py"
            
            cmd = [
                sys.executable, str(script_path),
                "--redis-url", self.redis_url,
                "--mode", mode,
                "--devices", str(devices)
            ]
            
            if mode == "continuous":
                cmd.extend(["--duration", str(duration)])
            else:
                cmd.extend(["--events", "1000"])
            
            logger.info(f"Starting data generator: {' '.join(cmd)}")
            self.generator_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Give it a moment to start
            time.sleep(2)
            
            if self.generator_process.poll() is None:
                logger.info("Data generator started successfully")
                return True
            else:
                logger.error("Data generator failed to start")
                return False
                
        except Exception as e:
            logger.error(f"Failed to start data generator: {e}")
            return False
    
    def start_bridge(self) -> bool:
        """Start the RDC-BigQuery bridge."""
        try:
            # Assuming the bridge can be run with python -m src.main
            cmd = [sys.executable, "-m", "src.main"]
            
            logger.info(f"Starting RDC-BigQuery bridge: {' '.join(cmd)}")
            self.bridge_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Give it a moment to start
            time.sleep(3)
            
            if self.bridge_process.poll() is None:
                logger.info("RDC-BigQuery bridge started successfully")
                return True
            else:
                logger.error("RDC-BigQuery bridge failed to start")
                return False
                
        except Exception as e:
            logger.error(f"Failed to start bridge: {e}")
            return False
    
    async def monitor_redis_activity(self, duration: int = 30) -> dict:
        """Monitor Redis activity during testing."""
        stats = {
            "keys_created": 0,
            "channel_messages": 0,
            "errors": 0
        }
        
        try:
            # Subscribe to keyspace notifications
            pubsub = self.redis_client.pubsub()
            await pubsub.psubscribe("__keyspace@0__:*")
            
            start_time = time.time()
            logger.info(f"Monitoring Redis activity for {duration} seconds...")
            
            while time.time() - start_time < duration:
                try:
                    message = await asyncio.wait_for(pubsub.get_message(), timeout=1.0)
                    if message and message['type'] == 'pmessage':
                        stats["keys_created"] += 1
                        if stats["keys_created"] % 10 == 0:
                            logger.info(f"Processed {stats['keys_created']} key events")
                            
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error monitoring Redis: {e}")
            
            await pubsub.unsubscribe()
            
        except Exception as e:
            logger.error(f"Failed to monitor Redis activity: {e}")
            stats["errors"] += 1
        
        return stats
    
    def stop_processes(self) -> None:
        """Stop all running processes."""
        if self.generator_process:
            try:
                self.generator_process.terminate()
                self.generator_process.wait(timeout=5)
                logger.info("Data generator stopped")
            except subprocess.TimeoutExpired:
                self.generator_process.kill()
                logger.warning("Data generator forcefully killed")
            except Exception as e:
                logger.error(f"Error stopping data generator: {e}")
        
        if self.bridge_process:
            try:
                self.bridge_process.terminate()
                self.bridge_process.wait(timeout=10)
                logger.info("RDC-BigQuery bridge stopped")
            except subprocess.TimeoutExpired:
                self.bridge_process.kill()
                logger.warning("RDC-BigQuery bridge forcefully killed")
            except Exception as e:
                logger.error(f"Error stopping bridge: {e}")
    
    async def run_integration_test(self, test_duration: int = 60) -> bool:
        """Run a complete integration test."""
        logger.info("=" * 60)
        logger.info("Starting RDC-BigQuery Bridge Integration Test")
        logger.info("=" * 60)
        
        success = True
        
        try:
            # 1. Setup Redis
            logger.info("Step 1: Setting up Redis...")
            if not await self.setup_redis():
                return False
            
            # 2. Clean up any existing test data
            logger.info("Step 2: Cleaning up existing test data...")
            await self.cleanup_redis()
            
            # 3. Start data generator
            logger.info("Step 3: Starting data generator...")
            if not self.start_data_generator(mode="continuous", duration=test_duration + 10):
                return False
            
            # 4. Start bridge
            logger.info("Step 4: Starting RDC-BigQuery bridge...")
            if not self.start_bridge():
                success = False
            else:
                # 5. Monitor activity
                logger.info("Step 5: Monitoring data flow...")
                stats = await self.monitor_redis_activity(test_duration)
                
                logger.info("Test Results:")
                logger.info(f"  - Key events processed: {stats['keys_created']}")
                logger.info(f"  - Channel messages: {stats['channel_messages']}")
                logger.info(f"  - Errors encountered: {stats['errors']}")
                
                if stats['keys_created'] == 0:
                    logger.warning("No key events detected - check data generator")
                    success = False
        
        except Exception as e:
            logger.error(f"Integration test failed: {e}")
            success = False
        
        finally:
            # 6. Cleanup
            logger.info("Step 6: Cleaning up...")
            self.stop_processes()
            await self.cleanup_redis()
        
        logger.info("=" * 60)
        if success:
            logger.info("Integration test PASSED")
        else:
            logger.error("Integration test FAILED")
        logger.info("=" * 60)
        
        return success


async def main():
    """Main function for test runner."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Runner for RDC-BigQuery Bridge")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0",
                       help="Redis connection URL")
    parser.add_argument("--duration", type=int, default=60,
                       help="Test duration in seconds")
    parser.add_argument("--mode", choices=["integration", "generator-only", "monitor-only"],
                       default="integration", help="Test mode")
    
    args = parser.parse_args()
    
    runner = TestRunner(args.redis_url)
    
    try:
        if args.mode == "integration":
            success = await runner.run_integration_test(args.duration)
            sys.exit(0 if success else 1)
        
        elif args.mode == "generator-only":
            logger.info("Running data generator only...")
            await runner.setup_redis()
            await runner.cleanup_redis()
            runner.start_data_generator(mode="continuous", duration=args.duration)
            
            # Wait for completion
            if runner.generator_process:
                runner.generator_process.wait()
        
        elif args.mode == "monitor-only":
            logger.info("Monitoring Redis activity only...")
            await runner.setup_redis()
            stats = await runner.monitor_redis_activity(args.duration)
            logger.info(f"Monitoring results: {stats}")
    
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        runner.stop_processes()
    except Exception as e:
        logger.error(f"Test runner error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

