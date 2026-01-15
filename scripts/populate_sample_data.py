"""Populate BigQuery with synthetic sample data for testing exports.

This script generates realistic sample data for all three tables:
- empatica: Biosensor data (heart rate, accelerometer, etc.)
- blueiot: Position tracking data
- global_state_events: Visitor state changes and device assignments

Usage:
    python scripts/populate_sample_data.py --tickets 10 --days 7
"""

import argparse
import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import List

from google.auth import load_credentials_from_file
from google.cloud import bigquery

from src.config import load_config, get_default_config_path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SampleDataGenerator:
    """Generates synthetic sample data for BigQuery tables."""
    
    def __init__(self, config):
        self.config = config
        self.bq_client = None
        
        # Sample ticket IDs
        self.ticket_ids = []
        
        # Sample device IDs
        self.device_ids = []
    
    async def initialize(self):
        """Initialize BigQuery client."""
        credentials, _ = load_credentials_from_file(
            self.config.gcp.credentials_file,
            scopes=[
                'https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        )
        
        self.bq_client = bigquery.Client(
            project=self.config.gcp.project_id,
            credentials=credentials
        )
        
        logger.info("BigQuery client initialized")
    
    def generate_ticket_ids(self, num_tickets: int) -> List[str]:
        """Generate sample ticket IDs."""
        self.ticket_ids = [f"TICKET{str(i+1).zfill(3)}" for i in range(num_tickets)]
        return self.ticket_ids
    
    def generate_device_ids(self, num_devices: int) -> List[str]:
        """Generate sample device IDs."""
        self.device_ids = [f"DEVICE_{chr(65 + i//26)}{chr(65 + i%26)}_{str(i+1).zfill(2)}" 
                          for i in range(num_devices)]
        return self.device_ids
    
    def generate_empatica_row(self, device_id: str, timestamp: datetime) -> dict:
        """Generate a single empatica biosensor row."""
        # Generate realistic biosensor data with some variation
        base_hr = random.uniform(60, 100)
        
        return {
            "event_timestamp": timestamp.isoformat(),
            "device_id": device_id,
            "heart_rate": round(base_hr + random.gauss(0, 5), 2),
            "skin_conductivity": round(random.uniform(0.1, 5.0), 3),
            "skin_temperature": round(random.uniform(32, 38), 2),
            "accelerometer": round(random.uniform(0, 2), 3),
            "engagement": round(random.uniform(0, 1), 3),
            "excitement": round(random.uniform(0, 1), 3),
            "stress": round(random.uniform(0, 1), 3),
            "battery_level": round(random.uniform(20, 100), 1)
        }
    
    def generate_blueiot_row(self, device_id: str, timestamp: datetime) -> dict:
        """Generate a single blueiot position row."""
        zone = random.choice(["Zone_A", "Zone_B", "Zone_C", "Lobby", "Exhibition_Hall"])
        
        # Generate position coordinates
        return {
            "event_timestamp": timestamp.isoformat(),
            "device_id": device_id,
            "position_x": round(random.uniform(0, 100), 2),
            "position_y": round(random.uniform(0, 100), 2),
            "position_z": round(random.uniform(0, 5), 2),
            "zone": zone
        }
    
    def generate_global_state_event_rows(self, ticket_id: str, device_id: str, 
                                        start_time: datetime) -> List[dict]:
        """Generate global_state_events for a ticket-device assignment."""
        rows = []
        
        # Device assignment event
        rows.append({
            "event_timestamp": start_time.isoformat(),
            "state_key": f"Visitors:{ticket_id}:EmpaticaDeviceID",
            "state_value": device_id
        })
        
        # Status event
        rows.append({
            "event_timestamp": start_time.isoformat(),
            "state_key": f"Visitors:{ticket_id}:Status",
            "state_value": "active"
        })
        
        # Random status changes throughout the day
        num_status_changes = random.randint(2, 5)
        statuses = ["active", "idle", "viewing", "interactive"]
        
        for i in range(num_status_changes):
            event_time = start_time + timedelta(minutes=random.randint(30, 360))
            rows.append({
                "event_timestamp": event_time.isoformat(),
                "state_key": f"Visitors:{ticket_id}:Status",
                "state_value": random.choice(statuses)
            })
        
        return rows
    
    async def populate_empatica(self, num_days: int, samples_per_hour: int = 60):
        """Populate empatica table with sample data."""
        logger.info(f"Generating empatica data for {len(self.device_ids)} devices over {num_days} days...")
        
        table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.empatica"
        
        rows = []
        start_date = datetime.now() - timedelta(days=num_days)
        
        for device_id in self.device_ids:
            # Generate data for each hour
            for day in range(num_days):
                for hour in range(24):
                    # Random visit duration (4-10 hours per day)
                    if random.random() < 0.4:  # 40% chance of activity each hour
                        for sample in range(samples_per_hour):
                            timestamp = start_date + timedelta(
                                days=day, 
                                hours=hour, 
                                minutes=sample
                            )
                            
                            rows.append(self.generate_empatica_row(device_id, timestamp))
                            
                            # Insert in batches
                            if len(rows) >= 1000:
                                errors = self.bq_client.insert_rows_json(table_id, rows)
                                if errors:
                                    logger.error(f"Errors inserting empatica data: {errors}")
                                else:
                                    logger.info(f"Inserted {len(rows)} empatica rows")
                                rows = []
        
        # Insert remaining rows
        if rows:
            errors = self.bq_client.insert_rows_json(table_id, rows)
            if errors:
                logger.error(f"Errors inserting empatica data: {errors}")
            else:
                logger.info(f"Inserted {len(rows)} empatica rows")
        
        logger.info("✓ Empatica data population complete")
    
    async def populate_blueiot(self, num_days: int, samples_per_hour: int = 60):
        """Populate blueiot table with sample data."""
        logger.info(f"Generating blueiot data for {len(self.device_ids)} devices over {num_days} days...")
        
        table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.blueiot"
        
        rows = []
        start_date = datetime.now() - timedelta(days=num_days)
        
        for device_id in self.device_ids:
            for day in range(num_days):
                for hour in range(24):
                    if random.random() < 0.4:  # Match empatica activity
                        for sample in range(samples_per_hour):
                            timestamp = start_date + timedelta(
                                days=day,
                                hours=hour,
                                minutes=sample
                            )
                            
                            rows.append(self.generate_blueiot_row(device_id, timestamp))
                            
                            if len(rows) >= 1000:
                                errors = self.bq_client.insert_rows_json(table_id, rows)
                                if errors:
                                    logger.error(f"Errors inserting blueiot data: {errors}")
                                else:
                                    logger.info(f"Inserted {len(rows)} blueiot rows")
                                rows = []
        
        if rows:
            errors = self.bq_client.insert_rows_json(table_id, rows)
            if errors:
                logger.error(f"Errors inserting blueiot data: {errors}")
            else:
                logger.info(f"Inserted {len(rows)} blueiot rows")
        
        logger.info("✓ BlueIoT data population complete")
    
    async def populate_global_state_events(self, num_days: int):
        """Populate global_state_events table with sample data."""
        logger.info(f"Generating global_state_events for {len(self.ticket_ids)} tickets...")
        
        table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.global_state_events"
        
        rows = []
        start_date = datetime.now() - timedelta(days=num_days)
        
        # Assign devices to tickets
        for i, ticket_id in enumerate(self.ticket_ids):
            # Each ticket gets one device
            device_id = self.device_ids[i % len(self.device_ids)]
            
            # Generate events for random days
            for day in range(num_days):
                if random.random() < 0.5:  # 50% chance of visit each day
                    visit_start = start_date + timedelta(
                        days=day,
                        hours=random.randint(8, 11)  # Visits start between 8am-11am
                    )
                    
                    rows.extend(self.generate_global_state_event_rows(
                        ticket_id, device_id, visit_start
                    ))
                    
                    if len(rows) >= 1000:
                        errors = self.bq_client.insert_rows_json(table_id, rows)
                        if errors:
                            logger.error(f"Errors inserting global_state_events: {errors}")
                        else:
                            logger.info(f"Inserted {len(rows)} global_state_events rows")
                        rows = []
        
        if rows:
            errors = self.bq_client.insert_rows_json(table_id, rows)
            if errors:
                logger.error(f"Errors inserting global_state_events: {errors}")
            else:
                logger.info(f"Inserted {len(rows)} global_state_events rows")
        
        logger.info("✓ Global state events population complete")
    
    async def populate_all(self, num_tickets: int, num_days: int):
        """Populate all tables with sample data."""
        await self.initialize()
        
        # Generate IDs
        self.generate_ticket_ids(num_tickets)
        self.generate_device_ids(num_tickets)  # One device per ticket
        
        logger.info(f"Generated {len(self.ticket_ids)} tickets and {len(self.device_ids)} devices")
        
        # Populate tables
        await self.populate_global_state_events(num_days)
        await self.populate_empatica(num_days, samples_per_hour=60)
        await self.populate_blueiot(num_days, samples_per_hour=60)
        
        logger.info("\n" + "="*80)
        logger.info("✓ Sample data population complete!")
        logger.info("="*80)
        logger.info(f"\nSummary:")
        logger.info(f"  Tickets: {len(self.ticket_ids)}")
        logger.info(f"  Devices: {len(self.device_ids)}")
        logger.info(f"  Days: {num_days}")
        logger.info(f"\nSample ticket IDs: {', '.join(self.ticket_ids[:5])}")
        logger.info(f"Sample device IDs: {', '.join(self.device_ids[:5])}")
        logger.info(f"\nYou can now test exports:")
        logger.info(f"  rdc-export list-tickets --start \"{(datetime.now() - timedelta(days=num_days)).strftime('%Y-%m-%d')}\" --end \"{datetime.now().strftime('%Y-%m-%d')}\"")
        logger.info(f"  rdc-export export-ticket --ticket-id {self.ticket_ids[0]} --format parquet")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Populate BigQuery with synthetic sample data for testing"
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to config file (default: config/config.yaml)'
    )
    
    parser.add_argument(
        '--tickets',
        type=int,
        default=10,
        help='Number of tickets to generate (default: 10)'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days of data to generate (default: 7)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load config
        config_path = args.config or get_default_config_path()
        config = load_config(config_path)
        
        # Generate data
        generator = SampleDataGenerator(config)
        await generator.populate_all(args.tickets, args.days)
        
    except Exception as e:
        logger.error(f"Failed to populate sample data: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    asyncio.run(main())