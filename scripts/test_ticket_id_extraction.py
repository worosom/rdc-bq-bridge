#!/usr/bin/env python3
"""Test script to verify ticket_id extraction from Visitors keyspace patterns."""

import asyncio
import json
import logging
import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config
from src.redis_data_event import RedisDataEvent
from src.row_processor import RowProcessor
from src.row_models import RowInProgress
from src.aggregation_handler import AggregationHandler

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_ticket_id_extraction():
    """Test the ticket_id extraction from various keyspace patterns."""

    # Load configuration
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    config = load_config(str(config_path))

    # Initialize components
    aggregation_handler = AggregationHandler(config.assembly.aggregation_rules)
    row_processor = RowProcessor(aggregation_handler)

    # Test cases for the new Visitors patterns
    test_cases = [
        {
            "name": "Visitor Status",
            "key": "Visitors:TICKET123:Status",
            "value": "active",
            "expected_ticket_id": "TICKET123",
            "routing_rule_name": "VisitorStatus"
        },
        {
            "name": "Visitor Empatica Device",
            "key": "Visitors:TICKET456:EmpaticaDeviceID",
            "value": "EMP789",
            "expected_ticket_id": "TICKET456",
            "routing_rule_name": "VisitorEmpaticaDevice"
        },
        {
            "name": "Visitor Scent Device",
            "key": "Visitors:TICKET789:ScentDeviceID",
            "value": "SCENT123",
            "expected_ticket_id": "TICKET789",
            "routing_rule_name": "VisitorScentDevice"
        },
        {
            "name": "Complex Ticket ID",
            "key": "Visitors:VISITOR-2024-001:Status",
            "value": "checked_in",
            "expected_ticket_id": "VISITOR-2024-001",
            "routing_rule_name": "VisitorStatus"
        }
    ]

    print("\n" + "="*60)
    print("Testing Ticket ID Extraction from Visitors Patterns")
    print("="*60 + "\n")

    for test_case in test_cases:
        print(f"\nTest Case: {test_case['name']}")
        print(f"  Key: {test_case['key']}")
        print(f"  Value: {test_case['value']}")

        # Find the appropriate routing rule
        routing_rule = None
        for rule in config.pipeline.routing_rules:
            if rule.name == test_case['routing_rule_name']:
                routing_rule = rule
                break

        if not routing_rule:
            print(f"  ❌ Routing rule '{test_case['routing_rule_name']}' not found!")
            continue

        # Create a RedisDataEvent
        event = RedisDataEvent(
            source_type="key",
            key_or_channel=test_case['key'],
            value=test_case['value'],
            routing_rule=routing_rule
        )

        # Extract row ID (which should be the ticket_id for these patterns)
        row_id = row_processor.extract_row_id(event)

        if row_id == test_case['expected_ticket_id']:
            print(f"  ✅ Correctly extracted ticket_id: {row_id}")
        else:
            print(f"  ❌ Failed to extract ticket_id!")
            print(f"     Expected: {test_case['expected_ticket_id']}")
            print(f"     Got: {row_id}")

        # Test data processing
        if row_id:
            row = RowInProgress(row_id=row_id, target_table="visitor_events")
            data_type = row_processor.extract_data_type(event)

            await row_processor.add_data_to_row(row, event, data_type)

            print(f"  Data type: {data_type}")
            print(f"  Row data after processing:")
            for key, value in row.data.items():
                print(f"    - {key}: {value}")

    print("\n" + "="*60)
    print("Testing Mixed Pattern Scenario (Ticket + Device)")
    print("="*60 + "\n")

    # Test a scenario where we have both ticket_id and device_id data
    row = RowInProgress(row_id="TICKET999", target_table="visitor_events")

    # First add ticket-based data
    ticket_events = [
        {
            "key": "Visitors:TICKET999:Status",
            "value": "active",
            "routing_rule_name": "VisitorStatus"
        },
        {
            "key": "Visitors:TICKET999:EmpaticaDeviceID",
            "value": "EMP999",
            "routing_rule_name": "VisitorEmpaticaDevice"
        }
    ]

    for event_data in ticket_events:
        routing_rule = next((r for r in config.pipeline.routing_rules
                            if r.name == event_data['routing_rule_name']), None)
        if routing_rule:
            event = RedisDataEvent(
                source_type="key",
                key_or_channel=event_data['key'],
                value=event_data['value'],
                routing_rule=routing_rule
            )
            data_type = row_processor.extract_data_type(event)
            await row_processor.add_data_to_row(row, event, data_type)

    # Now add device-based data (simulating wearable data)
    wearable_rule = next((r for r in config.pipeline.routing_rules
                         if r.name == "BioSensors"), None)
    if wearable_rule:
        biosensor_data = {
            "heart_rate": 75,
            "skin_temperature": 36.5,
            "skin_conductivity": 2.1
        }

        event = RedisDataEvent(
            source_type="channel",
            key_or_channel="Wearables:WatchDevices:WATCH123:BioSensors",
            value=json.dumps(biosensor_data),
            routing_rule=wearable_rule
        )

        data_type = row_processor.extract_data_type(event)
        await row_processor.add_data_to_row(row, event, data_type)

    print("Final row data with mixed sources:")
    for key, value in row.data.items():
        print(f"  - {key}: {value}")

    # Verify ticket_id is present
    if "ticket_id" in row.data:
        print(f"\n✅ ticket_id successfully stored: {row.data['ticket_id']}")
    else:
        print("\n❌ ticket_id not found in row data!")

    print("\n" + "="*60)
    print("Test completed!")
    print("="*60 + "\n")


if __name__ == "__main__":
    asyncio.run(test_ticket_id_extraction())

