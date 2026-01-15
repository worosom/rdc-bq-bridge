#!/usr/bin/env python3
"""
Redis Data Generator for RDC-BigQuery Bridge Testing

This script generates realistic test data to simulate the Redis Data Cache (RDC)
for testing the RDC-BigQuery bridge. It creates:
1. Visitor biometric data (channels)
2. Visitor position updates (keys)
3. Global state changes (keys)
"""

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import redis.asyncio as redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedisDataGenerator:
    """Generates realistic test data for Redis Data Cache."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        
        # Simulation parameters
        self.num_devices = 20  # Number of simulated watch devices
        self.device_ids = [f"watch_{i:03d}" for i in range(1, self.num_devices + 1)]
        
        # Room/zone definitions
        self.zones = [
            "MainHall", "ExhibitA", "ExhibitB", "ExhibitC", 
            "Restaurant", "Gift Shop", "Theater", "Lobby"
        ]
        
        # Global state keys
        self.global_state_keys = [
            "Rooms:MainHall:Audio:MasterVolume",
            "Rooms:MainHall:Lighting:Brightness",
            "Rooms:ExhibitA:Interactive:TouchScreen:State",
            "Rooms:ExhibitB:Projector:Status",
            "System:TotalVisitors",
            "System:ServerStatus",
            "Weather:Temperature",
            "Weather:Humidity"
        ]
        
        # Visitor state tracking
        self.visitor_states: Dict[str, Dict] = {}
        self._initialize_visitor_states()
        
        logger.info(f"Initialized data generator with {self.num_devices} devices")
    
    def _initialize_visitor_states(self) -> None:
        """Initialize realistic states for each visitor."""
        for device_id in self.device_ids:
            self.visitor_states[device_id] = {
                "zone": random.choice(self.zones),
                "position_x": random.uniform(0, 100),
                "position_y": random.uniform(0, 100),
                "heart_rate": random.uniform(60, 100),
                "skin_conductivity": random.uniform(0.1, 2.0),
                "skin_temperature": random.uniform(32.0, 37.0),
                "accelerometer_x": 0.0,
                "accelerometer_y": 0.0,
                "accelerometer_z": 9.8,
                "ticket_id": f"TICKET_{random.randint(100000, 999999)}",
                "last_movement": time.time()
            }
    
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
    
    async def generate_visitor_biosensors_data(self, device_id: str) -> Dict:
        """Generate realistic biometric sensor data for a visitor."""
        state = self.visitor_states[device_id]
        
        # Simulate realistic variations
        heart_rate_delta = random.uniform(-5, 5)
        state["heart_rate"] = max(50, min(150, state["heart_rate"] + heart_rate_delta))
        
        conductivity_delta = random.uniform(-0.1, 0.1)
        state["skin_conductivity"] = max(0.1, min(3.0, state["skin_conductivity"] + conductivity_delta))
        
        temp_delta = random.uniform(-0.2, 0.2)
        state["skin_temperature"] = max(30.0, min(40.0, state["skin_temperature"] + temp_delta))
        
        # Simulate movement with accelerometer data
        if random.random() < 0.3:  # 30% chance of movement
            state["accelerometer_x"] = random.uniform(-2.0, 2.0)
            state["accelerometer_y"] = random.uniform(-2.0, 2.0)
            state["accelerometer_z"] = random.uniform(8.0, 11.0)
            state["last_movement"] = time.time()
        else:
            # Gradually return to rest state
            state["accelerometer_x"] *= 0.8
            state["accelerometer_y"] *= 0.8
            state["accelerometer_z"] = 9.8 + (state["accelerometer_z"] - 9.8) * 0.9
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "device_id": device_id,
            "ticket_id": state.get("ticket_id", f"TICKET_{device_id}"),  # Add ticket_id
            "heart_rate": round(state["heart_rate"], 1),
            "skin_conductivity": round(state["skin_conductivity"], 3),
            "skin_temperature": round(state["skin_temperature"], 2),
            "accelerometer_x": round(state["accelerometer_x"], 3),
            "accelerometer_y": round(state["accelerometer_y"], 3),
            "accelerometer_z": round(state["accelerometer_z"], 3)
        }
    
    async def generate_visitor_position_data(self, device_id: str) -> Dict:
        """Generate position data for a visitor."""
        state = self.visitor_states[device_id]
        
        # Occasionally change zones (5% chance)
        if random.random() < 0.05:
            state["zone"] = random.choice(self.zones)
            # Reset position when changing zones
            state["position_x"] = random.uniform(0, 100)
            state["position_y"] = random.uniform(0, 100)
        else:
            # Small position adjustments within current zone
            pos_delta_x = random.uniform(-2, 2)
            pos_delta_y = random.uniform(-2, 2)
            state["position_x"] = max(0, min(100, state["position_x"] + pos_delta_x))
            state["position_y"] = max(0, min(100, state["position_y"] + pos_delta_y))
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "device_id": device_id,
            "ticket_id": state["ticket_id"],
            "position_x": round(state["position_x"], 2),
            "position_y": round(state["position_y"], 2),
            "zone": state["zone"]
        }
    
    async def generate_global_state_data(self) -> tuple[str, str]:
        """Generate global state change data."""
        key = random.choice(self.global_state_keys)
        
        # Generate appropriate values based on key type
        if "Volume" in key:
            value = str(random.randint(0, 100))
        elif "Brightness" in key:
            value = str(random.randint(0, 255))
        elif "Temperature" in key:
            value = str(round(random.uniform(18.0, 25.0), 1))
        elif "Humidity" in key:
            value = str(round(random.uniform(30.0, 70.0), 1))
        elif "TotalVisitors" in key:
            value = str(random.randint(50, 800))
        elif "Status" in key or "State" in key:
            value = random.choice(["active", "inactive", "maintenance", "error"])
        else:
            value = str(random.randint(0, 100))
        
        return key, value
    
    async def publish_biosensor_data(self) -> None:
        """Publish biometric data to Redis channels."""
        tasks = []
        for device_id in self.device_ids:
            # Only publish for ~80% of devices (simulate some offline)
            if random.random() < 0.8:
                tasks.append(self._publish_single_biosensor(device_id))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _publish_single_biosensor(self, device_id: str) -> None:
        """Publish biosensor data for a single device."""
        try:
            data = await self.generate_visitor_biosensors_data(device_id)
            channel = f"Wearables:WatchDevices:{device_id}:BioSensors"
            
            await self.redis_client.publish(channel, json.dumps(data))
            logger.debug(f"Published biosensor data for {device_id}")
            
        except Exception as e:
            logger.error(f"Error publishing biosensor data for {device_id}: {e}")
    
    async def update_position_data(self) -> None:
        """Update position data in Redis keys."""
        tasks = []
        for device_id in self.device_ids:
            # Update positions less frequently (every ~3 seconds on average)
            if random.random() < 0.33:
                tasks.append(self._update_single_position(device_id))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _update_single_position(self, device_id: str) -> None:
        """Update position data for a single device."""
        try:
            data = await self.generate_visitor_position_data(device_id)
            key = f"Wearables:WatchDevices:{device_id}:Position"
            
            await self.redis_client.set(key, json.dumps(data))
            logger.debug(f"Updated position for {device_id}")
            
        except Exception as e:
            logger.error(f"Error updating position for {device_id}: {e}")
    
    async def update_global_state(self) -> None:
        """Update global state keys."""
        # Update 1-3 global state keys
        num_updates = random.randint(1, 3)
        
        for _ in range(num_updates):
            try:
                key, value = await self.generate_global_state_data()
                await self.redis_client.set(key, value)
                logger.debug(f"Updated global state: {key} = {value}")
                
            except Exception as e:
                logger.error(f"Error updating global state: {e}")
    
    async def run_continuous_generation(self, duration_seconds: int = 300) -> None:
        """Run continuous data generation for specified duration."""
        logger.info(f"Starting continuous data generation for {duration_seconds} seconds")
        
        start_time = time.time()
        iteration = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                iteration += 1
                iteration_start = time.time()
                
                # Generate data concurrently
                await asyncio.gather(
                    self.publish_biosensor_data(),
                    self.update_position_data(),
                    self.update_global_state(),
                    return_exceptions=True
                )
                
                # Log progress every 30 seconds
                if iteration % 30 == 0:
                    elapsed = time.time() - start_time
                    logger.info(f"Generated data for {elapsed:.1f}s ({iteration} iterations)")
                
                # Maintain ~1 second intervals
                iteration_time = time.time() - iteration_start
                sleep_time = max(0, 1.0 - iteration_time)
                await asyncio.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Data generation interrupted by user")
        
        total_time = time.time() - start_time
        logger.info(f"Data generation completed. Total time: {total_time:.1f}s, Iterations: {iteration}")
    
    async def generate_burst_data(self, num_events: int = 1000) -> None:
        """Generate a burst of test data quickly."""
        logger.info(f"Generating burst of {num_events} events")
        
        events_per_batch = 50
        batches = (num_events + events_per_batch - 1) // events_per_batch
        
        for batch in range(batches):
            batch_start = time.time()
            tasks = []
            
            # Generate mixed data types in each batch
            for _ in range(events_per_batch):
                event_type = random.choices(
                    ["biosensor", "position", "global"],
                    weights=[0.7, 0.2, 0.1]  # 70% biosensor, 20% position, 10% global
                )[0]
                
                if event_type == "biosensor":
                    device_id = random.choice(self.device_ids)
                    tasks.append(self._publish_single_biosensor(device_id))
                elif event_type == "position":
                    device_id = random.choice(self.device_ids)
                    tasks.append(self._update_single_position(device_id))
                else:  # global
                    tasks.append(self._update_single_global_state())
            
            # Execute batch
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Small delay between batches
            batch_time = time.time() - batch_start
            if batch_time < 0.1:
                await asyncio.sleep(0.1 - batch_time)
            
            if (batch + 1) % 10 == 0:
                logger.info(f"Generated {(batch + 1) * events_per_batch} events")
        
        logger.info(f"Burst generation completed: {num_events} events")
    
    async def _update_single_global_state(self) -> None:
        """Update a single global state key."""
        try:
            key, value = await self.generate_global_state_data()
            await self.redis_client.set(key, value)
        except Exception as e:
            logger.error(f"Error updating global state: {e}")


async def main():
    """Main function for running the data generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Redis Data Generator for RDC-BigQuery Bridge")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0",
                       help="Redis connection URL")
    parser.add_argument("--mode", choices=["continuous", "burst"], default="continuous",
                       help="Generation mode")
    parser.add_argument("--duration", type=int, default=300,
                       help="Duration in seconds for continuous mode")
    parser.add_argument("--events", type=int, default=1000,
                       help="Number of events for burst mode")
    parser.add_argument("--devices", type=int, default=20,
                       help="Number of simulated devices")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create generator
    generator = RedisDataGenerator(args.redis_url)
    generator.num_devices = args.devices
    generator.device_ids = [f"watch_{i:03d}" for i in range(1, args.devices + 1)]
    generator._initialize_visitor_states()
    
    try:
        await generator.connect()
        
        if args.mode == "continuous":
            await generator.run_continuous_generation(args.duration)
        else:  # burst
            await generator.generate_burst_data(args.events)
            
    except Exception as e:
        logger.error(f"Error during data generation: {e}")
    finally:
        await generator.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

