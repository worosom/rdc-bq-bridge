"""Command-line interface for BigQuery exports."""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from ..config import load_config, get_default_config_path
from .bq_exporter import BigQueryExporter
from .export_config import ExportConfig, TimeRangeExport, TicketExport

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_datetime(date_str: str) -> datetime:
    """Parse datetime string in various formats with optional timezone support.
    
    Supports:
    - ISO 8601 with timezone: "2025-01-09T10:30:00+00:00", "2025-01-09T10:30:00Z"
    - ISO 8601 with named timezone: "2025-01-09T10:30:00[America/New_York]"
    - ISO 8601 without timezone: "2025-01-09T10:30:00"
    - Date with space separator: "2025-01-09 10:30:00"
    - Date only: "2025-01-09"
    
    If no timezone is specified, the datetime is returned as naive (no timezone).
    """
    # First try fromisoformat for full ISO 8601 support including timezones
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass
    
    # Try named timezone format: "2025-01-09T10:30:00[America/New_York]"
    if '[' in date_str and ']' in date_str:
        try:
            dt_part, tz_part = date_str.rsplit('[', 1)
            tz_name = tz_part.rstrip(']')
            dt = datetime.fromisoformat(dt_part)
            return dt.replace(tzinfo=ZoneInfo(tz_name))
        except (ValueError, Exception):
            pass
    
    # Fall back to legacy formats (without timezone)
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(
        f"Invalid datetime format: {date_str}. "
        f"Supported formats: ISO 8601 (with/without timezone), YYYY-MM-DD HH:MM:SS, YYYY-MM-DD"
    )


async def export_timerange(args: argparse.Namespace) -> None:
    """Export data within a time range."""
    try:
        # Load config
        config_path = args.config or get_default_config_path()
        config = load_config(config_path)
        
        # Parse times
        start_time = parse_datetime(args.start)
        end_time = parse_datetime(args.end)
        
        # Parse tables
        tables = args.tables.split(',') if args.tables else ["empatica", "blueiot", "global_state_events"]
        
        # Create export spec
        export_spec = TimeRangeExport(
            start_time=start_time,
            end_time=end_time,
            tables=tables,
            format=args.format,
            output_path=Path(args.output) if args.output else None,
            include_ticket_id=not args.no_ticket_id
        )
        
        # Create exporter
        export_config = ExportConfig(
            output_dir=Path(args.output_dir) if hasattr(args, 'output_dir') and args.output_dir else Path("./exports"),
            parquet_compression=args.compression if args.format == "parquet" else "snappy",
        )
        
        exporter = BigQueryExporter(config, export_config)
        
        # Execute export
        output_files = await exporter.export_time_range(export_spec)
        
        # Print results
        print("\n✓ Export completed successfully!")
        print(f"\nExported files:")
        for table, path in output_files.items():
            print(f"  - {table}: {path}")
        
    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        sys.exit(1)


async def export_ticket(args: argparse.Namespace) -> None:
    """Export data for a specific ticket."""
    try:
        # Load config
        config_path = args.config or get_default_config_path()
        config = load_config(config_path)
        
        # Parse optional times
        start_time = parse_datetime(args.start) if args.start else None
        end_time = parse_datetime(args.end) if args.end else None
        
        # Parse tables
        tables = args.tables.split(',') if args.tables else ["empatica", "blueiot", "global_state_events"]
        
        # Create export spec
        export_spec = TicketExport(
            ticket_id=args.ticket_id,
            start_time=start_time,
            end_time=end_time,
            tables=tables,
            format=args.format,
            output_path=Path(args.output) if args.output else None
        )
        
        # Create exporter
        export_config = ExportConfig(
            output_dir=Path(args.output_dir) if hasattr(args, 'output_dir') and args.output_dir else Path("./exports"),
            parquet_compression=args.compression if args.format == "parquet" else "snappy",
        )
        
        exporter = BigQueryExporter(config, export_config)
        
        # Execute export
        output_files = await exporter.export_by_ticket(export_spec)
        
        # Print results
        print("\n✓ Export completed successfully!")
        print(f"\nTicket: {args.ticket_id}")
        print(f"Exported files:")
        for table, path in output_files.items():
            print(f"  - {table}: {path}")
        
    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        sys.exit(1)


async def list_tickets(args: argparse.Namespace) -> None:
    """List available tickets in the dataset."""
    try:
        # Load config
        config_path = args.config or get_default_config_path()
        config = load_config(config_path)
        
        # Create exporter
        exporter = BigQueryExporter(config)
        
        # Get tickets
        tickets = await exporter.list_available_tickets(
            start_time=args.start if args.start else None,
            end_time=args.end if args.end else None
        )
        
        # Print results
        if not tickets:
            print("No tickets found in the specified time range.")
            return
        
        print(f"\nFound {len(tickets)} tickets:\n")
        print(f"{'Ticket ID':<20} {'First Seen':<25} {'Last Seen':<25} {'Events':<10}")
        print("-" * 85)
        
        for ticket in tickets:
            ticket_id = ticket['ticket_id']
            first_seen = ticket['first_seen'].strftime("%Y-%m-%d %H:%M:%S")
            last_seen = ticket['last_seen'].strftime("%Y-%m-%d %H:%M:%S")
            event_count = ticket['event_count']
            
            print(f"{ticket_id:<20} {first_seen:<25} {last_seen:<25} {event_count:<10}")
        
    except Exception as e:
        logger.error(f"Failed to list tickets: {e}", exc_info=True)
        sys.exit(1)


async def debug_ticket(args: argparse.Namespace) -> None:
    """Debug device mappings for a specific ticket."""
    try:
        # Load config
        config_path = args.config or get_default_config_path()
        config = load_config(config_path)
        
        # Create exporter
        exporter = BigQueryExporter(config)
        await exporter.initialize()
        
        # Query device mappings
        query = f"""
SELECT
    event_timestamp,
    state_key,
    state_value as device_id
FROM `{config.gcp.project_id}.{config.gcp.dataset_id}.global_state_events`
WHERE state_key = 'Visitors:{args.ticket_id}:EmpaticaDeviceID'
ORDER BY event_timestamp ASC
"""
        
        print(f"\nDebug info for ticket: {args.ticket_id}\n")
        print("Device mappings in global_state_events:")
        print("-" * 80)
        
        df = await exporter._execute_query_to_dataframe(query)
        
        if len(df) == 0:
            print(f"No device mappings found for ticket '{args.ticket_id}'")
            print("\nPossible issues:")
            print("  1. Ticket ID doesn't exist in the database")
            print("  2. No EmpaticaDeviceID was ever assigned to this ticket")
            print("\nTry running: rdc-export list-tickets")
            return
        
        print(f"\nFound {len(df)} device mapping event(s):\n")
        for _, row in df.iterrows():
            timestamp = row['event_timestamp'].strftime("%Y-%m-%d %H:%M:%S")
            device_id = row['device_id']
            print(f"  {timestamp}  →  Device: {device_id}")
        
        # Now check if there's data for these devices
        device_ids = df['device_id'].unique().tolist()
        # Filter out empty device IDs
        device_ids = [d for d in device_ids if d and d.strip()]
        
        if not device_ids:
            print("\nNo valid device IDs found (all device_ids are empty)")
            return
        
        print(f"\n\nChecking for biometric data for device(s): {device_ids}\n")
        
        for table in ['empatica', 'blueiot']:
            device_list = "', '".join(device_ids)
            
            # Get count
            count_query = f"""
SELECT COUNT(*) as count
FROM `{config.gcp.project_id}.{config.gcp.dataset_id}.{table}`
WHERE device_id IN ('{device_list}')
"""
            count_df = await exporter._execute_query_to_dataframe(count_query)
            count = count_df.iloc[0]['count']
            
            # Get timestamp range
            range_query = f"""
SELECT 
    MIN(event_timestamp) as earliest,
    MAX(event_timestamp) as latest
FROM `{config.gcp.project_id}.{config.gcp.dataset_id}.{table}`
WHERE device_id IN ('{device_list}')
"""
            range_df = await exporter._execute_query_to_dataframe(range_query)
            
            if count > 0:
                earliest = range_df.iloc[0]['earliest']
                latest = range_df.iloc[0]['latest']
                print(f"  {table}: {count} rows")
                print(f"    Time range: {earliest.strftime('%Y-%m-%d %H:%M:%S')} to {latest.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Compare with device assignment times
                print(f"    Device assignment times:")
                for _, row in df.iterrows():
                    if row['device_id'] in device_ids:
                        assign_time = row['event_timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                        print(f"      Assigned at: {assign_time}")
                        if earliest < row['event_timestamp']:
                            print(f"      ⚠ WARNING: Biometric data exists BEFORE assignment time!")
            else:
                print(f"  {table}: {count} rows")
        
    except Exception as e:
        logger.error(f"Debug failed: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Export data from BigQuery for data scientists and Redis replay",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export by time range (Parquet for data scientists)
  python -m src.exporter.cli export-timerange \\
    --start "2025-12-01T00:00:00" \\
    --end "2025-12-10T23:59:59" \\
    --tables empatica,blueiot \\
    --format parquet
  
  # Export by ticket (Avro for Redis replay)
  python -m src.exporter.cli export-ticket \\
    --ticket-id TICKET123 \\
    --format avro
  
  # List available tickets
  python -m src.exporter.cli list-tickets \\
    --start "2025-12-01" \\
    --end "2025-12-10"
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to config file (default: config/config.yaml)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # export-timerange command
    timerange_parser = subparsers.add_parser(
        'export-timerange',
        help='Export data within a time range'
    )
    timerange_parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='Start time (ISO format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)'
    )
    timerange_parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='End time (ISO format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)'
    )
    timerange_parser.add_argument(
        '--tables',
        type=str,
        help='Comma-separated list of tables (default: empatica,blueiot,global_state_events)'
    )
    timerange_parser.add_argument(
        '--format',
        type=str,
        choices=['parquet', 'avro', 'jsonl'],
        default='parquet',
        help='Output format (default: parquet)'
    )
    timerange_parser.add_argument(
        '--output',
        type=str,
        help='Output path (file or directory)'
    )
    timerange_parser.add_argument(
        '--output-dir',
        type=str,
        help='Base output directory (default: /data/exports)'
    )
    timerange_parser.add_argument(
        '--compression',
        type=str,
        choices=['snappy', 'gzip', 'zstd'],
        default='snappy',
        help='Compression codec for Parquet (default: snappy)'
    )
    timerange_parser.add_argument(
        '--no-ticket-id',
        action='store_true',
        help='Do not enrich biometric data with ticket_id'
    )
    
    # export-ticket command
    ticket_parser = subparsers.add_parser(
        'export-ticket',
        help='Export data for a specific ticket'
    )
    ticket_parser.add_argument(
        '--ticket-id',
        type=str,
        required=True,
        help='Ticket ID to export'
    )
    ticket_parser.add_argument(
        '--start',
        type=str,
        help='Optional start time filter (ISO format)'
    )
    ticket_parser.add_argument(
        '--end',
        type=str,
        help='Optional end time filter (ISO format)'
    )
    ticket_parser.add_argument(
        '--tables',
        type=str,
        help='Comma-separated list of tables (default: empatica,blueiot,global_state_events)'
    )
    ticket_parser.add_argument(
        '--format',
        type=str,
        choices=['parquet', 'avro', 'jsonl'],
        default='parquet',
        help='Output format (default: parquet)'
    )
    ticket_parser.add_argument(
        '--output',
        type=str,
        help='Output path (file or directory)'
    )
    ticket_parser.add_argument(
        '--output-dir',
        type=str,
        help='Base output directory (default: /data/exports)'
    )
    ticket_parser.add_argument(
        '--compression',
        type=str,
        choices=['snappy', 'gzip', 'zstd'],
        default='snappy',
        help='Compression codec for Parquet (default: snappy)'
    )
    
    # list-tickets command
    list_parser = subparsers.add_parser(
        'list-tickets',
        help='List available tickets'
    )
    list_parser.add_argument(
        '--start',
        type=str,
        help='Start time filter (ISO format)'
    )
    list_parser.add_argument(
        '--end',
        type=str,
        help='End time filter (ISO format)'
    )
    
    # debug-ticket command
    debug_parser = subparsers.add_parser(
        'debug-ticket',
        help='Debug device mappings for a specific ticket'
    )
    debug_parser.add_argument(
        '--ticket-id',
        type=str,
        required=True,
        help='Ticket ID to debug'
    )
    
    args = parser.parse_args()
    
    # Enable debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Execute command
    if args.command == 'export-timerange':
        asyncio.run(export_timerange(args))
    elif args.command == 'export-ticket':
        asyncio.run(export_ticket(args))
    elif args.command == 'list-tickets':
        asyncio.run(list_tickets(args))
    elif args.command == 'debug-ticket':
        asyncio.run(debug_ticket(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()