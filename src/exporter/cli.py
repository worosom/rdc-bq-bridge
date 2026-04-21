"""Command-line interface for BigQuery exports."""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
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


def parse_datetime(date_str: str, default_tz: str = "America/Los_Angeles") -> datetime:
    """Parse datetime string in various formats with optional timezone support.
    
    Supports:
    - ISO 8601 with timezone: "2025-01-09T10:30:00+00:00", "2025-01-09T10:30:00Z"
    - ISO 8601 with named timezone: "2025-01-09T10:30:00[America/New_York]"
    - ISO 8601 without timezone: "2025-01-09T10:30:00" (defaults to specified timezone)
    - Date with space separator: "2025-01-09 10:30:00" (defaults to specified timezone)
    - Date only: "2025-01-09" (defaults to specified timezone at 00:00:00)
    
    Args:
        date_str: Date/time string to parse
        default_tz: Default timezone to use if none specified (default: "America/Los_Angeles")
    
    If no timezone is specified in the date string, the default_tz is used.
    """
    default_tzinfo = ZoneInfo(default_tz)
    
    # First try fromisoformat for full ISO 8601 support including timezones
    try:
        dt = datetime.fromisoformat(date_str)
        # If no timezone specified, apply default timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=default_tzinfo)
        return dt
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
    
    # Fall back to legacy formats (without timezone) - apply default timezone
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Apply default timezone
            return dt.replace(tzinfo=default_tzinfo)
        except ValueError:
            continue
    
    raise ValueError(
        f"Invalid datetime format: {date_str}. "
        f"Supported formats: ISO 8601 (with/without timezone), YYYY-MM-DD HH:MM:SS, YYYY-MM-DD. "
        f"Dates without timezone default to {default_tz}."
    )


async def export_timerange(args: argparse.Namespace) -> None:
    """Export data within a time range."""
    try:
        # Load config
        config_path = args.config or get_default_config_path()
        config = load_config(config_path)
        
        # Parse times
        start_time = parse_datetime(args.start, args.tz)
        end_time = parse_datetime(args.end, args.tz)
        
        # Parse tables
        tables = args.tables.split(',') if args.tables else ["empatica", "blueiot", "global_state_events"]
        
        # Create export spec
        export_spec = TimeRangeExport(
            start_time=start_time,
            end_time=end_time,
            tables=tables,
            format=args.format,
            output_path=Path(args.output) if args.output else None,
            include_ticket_id=not args.no_ticket_id,
            include_initial_state=not args.no_initial_state
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
        start_time = parse_datetime(args.start, args.tz) if args.start else None
        end_time = parse_datetime(args.end, args.tz) if args.end else None
        
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
        
        # Parse optional times
        start_time_str = None
        end_time_str = None
        
        if args.start:
            start_time = parse_datetime(args.start, args.tz)
            start_time_str = start_time.isoformat()
        
        if args.end:
            end_time = parse_datetime(args.end, args.tz)
            end_time_str = end_time.isoformat()
        
        # Create exporter
        exporter = BigQueryExporter(config)
        
        # Get tickets
        tickets = await exporter.list_available_tickets(
            start_time=start_time_str,
            end_time=end_time_str
        )
        
        # Print results
        if not tickets:
            print("No tickets found in the specified time range.")
            return
        
        # Get timezone for display
        display_tz = ZoneInfo(args.tz)
        
        print(f"\nFound {len(tickets)} tickets (times shown in {args.tz}):\n")
        print(f"{'Ticket ID':<36} {'First Seen':<20} {'Last Seen':<20} {'Events':<10}")
        print("-" * 86)
        
        for ticket in tickets:
            ticket_id = ticket['ticket_id']
            # Convert to display timezone
            first_seen = ticket['first_seen'].astimezone(display_tz).strftime("%Y-%m-%d %H:%M:%S")
            last_seen = ticket['last_seen'].astimezone(display_tz).strftime("%Y-%m-%d %H:%M:%S")
            event_count = ticket['event_count']
            
            print(f"{ticket_id:<36} {first_seen:<20} {last_seen:<20} {event_count:<10}")
        
    except Exception as e:
        logger.error(f"Failed to list tickets: {e}", exc_info=True)
        sys.exit(1)


async def debug_ticket(args: argparse.Namespace) -> None:
    """Debug ticket data and device mappings."""
    try:
        # Load config
        config_path = args.config or get_default_config_path()
        config = load_config(config_path)
        
        # Create exporter
        exporter = BigQueryExporter(config)
        await exporter.initialize()
        
        print(f"\nDebug info for ticket: {args.ticket_id}\n")
        
        # Check each table for data with this ticket_id
        for table in ['empatica', 'blueiot', 'global_state_events']:
            # Get count and time range
            query = f"""
SELECT 
    COUNT(*) as count,
    MIN(event_timestamp) as earliest,
    MAX(event_timestamp) as latest
FROM `{config.gcp.project_id}.{config.gcp.dataset_id}.{table}`
WHERE ticket_id = '{args.ticket_id}'
"""
            df = await exporter._execute_query_to_dataframe(query)
            
            count = df.iloc[0]['count']
            
            if count > 0:
                earliest = df.iloc[0]['earliest']
                latest = df.iloc[0]['latest']
                print(f"{table}:")
                print(f"  Rows: {count}")
                print(f"  Time range: {earliest.strftime('%Y-%m-%d %H:%M:%S')} to {latest.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # For biometric tables, show unique device_ids
                if table in ['empatica', 'blueiot']:
                    device_query = f"""
SELECT DISTINCT device_id
FROM `{config.gcp.project_id}.{config.gcp.dataset_id}.{table}`
WHERE ticket_id = '{args.ticket_id}'
ORDER BY device_id
"""
                    device_df = await exporter._execute_query_to_dataframe(device_query)
                    device_ids = device_df['device_id'].tolist()
                    print(f"  Devices: {', '.join(device_ids)}")
                
                # For global_state_events, show sample state keys
                if table == 'global_state_events':
                    keys_query = f"""
SELECT DISTINCT state_key
FROM `{config.gcp.project_id}.{config.gcp.dataset_id}.{table}`
WHERE ticket_id = '{args.ticket_id}'
ORDER BY state_key
LIMIT 10
"""
                    keys_df = await exporter._execute_query_to_dataframe(keys_query)
                    state_keys = keys_df['state_key'].tolist()
                    print(f"  Sample state keys ({len(state_keys)}):")
                    for key in state_keys:
                        print(f"    - {key}")
                
                print()
            else:
                print(f"{table}: No data found\n")
        
        # Summary
        print("\nSummary:")
        print("  If no data was found, the ticket_id may not exist in the database.")
        print("  Try running: rdc-export list-tickets")
        
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
  
  # List available tickets with time filtering
  python -m src.exporter.cli list-tickets \\
    --start "2025-12-01T00:00:00" \\
    --end "2025-12-10T23:59:59"
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
    
    parser.add_argument(
        '--tz',
        type=str,
        default='America/Los_Angeles',
        help='Default timezone for datetime inputs (default: America/Los_Angeles for PST)'
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
    timerange_parser.add_argument(
        '--no-initial-state',
        action='store_true',
        help='Do not include initial state snapshot before the time range (for Avro replay exports)'
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