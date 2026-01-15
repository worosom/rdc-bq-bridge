#!/usr/bin/env python3
"""
BigQuery Query Examples for Visitor Events

This script demonstrates how to query visitor events from BigQuery using Python.
"""

from google.cloud import bigquery
import os

# Configuration
PROJECT_ID = "dataland-backend"
DATASET_ID = "dl_staging"
TABLE_ID = "visitor_events"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Set up credentials (update path as needed)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/alex/dataland-backend-rdc-bq-bridge-sa-2dfde8a1840f.json"


def get_visitor_events(device_id: str, limit: int = 100):
    """Get all events for a specific visitor."""
    client = bigquery.Client()

    query = f"""
    SELECT 
        event_timestamp,
        device_id,
        ticket_id,
        position_x,
        position_y,
        zone,
        heart_rate,
        skin_conductivity,
        skin_temperature,
        accelerometer_x,
        accelerometer_y,
        accelerometer_z
    FROM 
        `{FULL_TABLE_ID}`
    WHERE 
        device_id = @device_id
    ORDER BY 
        event_timestamp DESC
    LIMIT {limit}
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("device_id", "STRING", device_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    # Convert to pandas DataFrame
    df = results.to_dataframe()
    return df


def get_recent_visitor_events(device_id: str, hours: int = 24):
    """Get recent events for a visitor within the last N hours."""
    client = bigquery.Client()

    query = f"""
    SELECT 
        event_timestamp,
        device_id,
        ticket_id,
        position_x,
        position_y,
        zone,
        heart_rate,
        skin_conductivity,
        skin_temperature
    FROM 
        `{FULL_TABLE_ID}`
    WHERE 
        device_id = @device_id
        AND event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
    ORDER BY 
        event_timestamp DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("device_id", "STRING", device_id),
            bigquery.ScalarQueryParameter("hours", "INT64", hours)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    df = results.to_dataframe()
    return df


def get_visitor_summary(device_id: str):
    """Get a summary of visitor's biometric data and activity."""
    client = bigquery.Client()

    query = f"""
    SELECT 
        device_id,
        ticket_id,
        COUNT(*) as total_events,
        MIN(event_timestamp) as first_event,
        MAX(event_timestamp) as last_event,
        AVG(heart_rate) as avg_heart_rate,
        MIN(heart_rate) as min_heart_rate,
        MAX(heart_rate) as max_heart_rate,
        AVG(skin_conductivity) as avg_skin_conductivity,
        AVG(skin_temperature) as avg_skin_temperature,
        COUNT(DISTINCT zone) as zones_visited
    FROM 
        `{FULL_TABLE_ID}`
    WHERE 
        device_id = @device_id
    GROUP BY 
        device_id, ticket_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("device_id", "STRING", device_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    df = results.to_dataframe()
    return df


def get_visitor_zone_history(device_id: str):
    """Get the visitor's path through different zones."""
    client = bigquery.Client()

    query = f"""
    WITH zone_transitions AS (
        SELECT 
            device_id,
            ticket_id,
            zone,
            event_timestamp,
            LAG(zone) OVER (PARTITION BY device_id ORDER BY event_timestamp) as previous_zone
        FROM 
            `{FULL_TABLE_ID}`
        WHERE 
            device_id = @device_id
            AND zone IS NOT NULL
    )
    SELECT 
        event_timestamp,
        previous_zone,
        zone as current_zone,
        TIMESTAMP_DIFF(
            event_timestamp,
            LAG(event_timestamp) OVER (ORDER BY event_timestamp),
            MINUTE
        ) as minutes_in_previous_zone
    FROM 
        zone_transitions
    WHERE 
        zone != IFNULL(previous_zone, '')  -- Only show zone changes
    ORDER BY 
        event_timestamp DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("device_id", "STRING", device_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    df = results.to_dataframe()
    return df


def get_all_visitors_latest_status():
    """Get the latest status of all visitors."""
    client = bigquery.Client()

    query = f"""
    WITH latest_events AS (
        SELECT 
            device_id,
            MAX(event_timestamp) as latest_timestamp
        FROM 
            `{FULL_TABLE_ID}`
        WHERE 
            event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        GROUP BY 
            device_id
    )
    SELECT 
        ve.device_id,
        ve.ticket_id,
        ve.event_timestamp as last_seen,
        ve.zone as current_zone,
        ve.heart_rate,
        ve.skin_temperature,
        ve.position_x,
        ve.position_y
    FROM 
        `{FULL_TABLE_ID}` ve
    INNER JOIN 
        latest_events le 
        ON ve.device_id = le.device_id 
        AND ve.event_timestamp = le.latest_timestamp
    ORDER BY 
        ve.event_timestamp DESC
    """

    query_job = client.query(query)
    results = query_job.result()

    df = results.to_dataframe()
    return df


def main():
    """Example usage of the query functions."""

    # Example device ID - replace with actual device ID
    device_id = "device_001"

    print("=" * 60)
    print("VISITOR EVENTS QUERY EXAMPLES")
    print("=" * 60)

    try:
        # Get recent events
        print(f"\n1. Recent events for device {device_id}:")
        recent_events = get_recent_visitor_events(device_id, hours=24)
        if not recent_events.empty:
            print(recent_events.head())
        else:
            print("No recent events found")

        # Get visitor summary
        print(f"\n2. Summary for device {device_id}:")
        summary = get_visitor_summary(device_id)
        if not summary.empty:
            print(summary)
        else:
            print("No data found for this device")

        # Get zone history
        print(f"\n3. Zone history for device {device_id}:")
        zone_history = get_visitor_zone_history(device_id)
        if not zone_history.empty:
            print(zone_history.head())
        else:
            print("No zone history found")

        # Get all visitors latest status
        print("\n4. Latest status of all active visitors:")
        all_visitors = get_all_visitors_latest_status()
        if not all_visitors.empty:
            print(all_visitors.head(10))
        else:
            print("No active visitors found")

    except Exception as e:
        print(f"Error querying BigQuery: {e}")
        print("\nMake sure to:")
        print("1. Update the device_id variable with an actual device ID")
        print("2. Ensure the credentials file path is correct")
        print("3. Verify that data exists in the BigQuery table")


if __name__ == "__main__":
    main()
