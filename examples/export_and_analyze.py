"""Example: Export data from BigQuery and perform basic analysis."""

import asyncio
from datetime import datetime
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from src.config import load_config
from src.exporter import BigQueryExporter, ExportConfig, TimeRangeExport, TicketExport


async def example_list_tickets():
    """Example: List all available tickets in a time range."""
    print("=" * 80)
    print("Example 1: List Available Tickets")
    print("=" * 80)
    
    # Load configuration
    config = load_config('config/config.yaml')
    
    # Create exporter
    exporter = BigQueryExporter(config)
    
    # List tickets
    tickets = await exporter.list_available_tickets(
        start_time="2025-12-01T00:00:00",
        end_time="2025-12-10T23:59:59"
    )
    
    print(f"\nFound {len(tickets)} tickets:\n")
    print(f"{'Ticket ID':<20} {'First Seen':<25} {'Last Seen':<25} {'Events':<10}")
    print("-" * 85)
    
    for ticket in tickets[:10]:  # Show first 10
        ticket_id = ticket['ticket_id']
        first_seen = ticket['first_seen'].strftime("%Y-%m-%d %H:%M:%S")
        last_seen = ticket['last_seen'].strftime("%Y-%m-%d %H:%M:%S")
        event_count = ticket['event_count']
        
        print(f"{ticket_id:<20} {first_seen:<25} {last_seen:<25} {event_count:<10}")
    
    return tickets


async def example_export_timerange():
    """Example: Export data for a time range (Parquet format)."""
    print("\n" + "=" * 80)
    print("Example 2: Export Time Range (Parquet for Data Scientists)")
    print("=" * 80)
    
    # Load configuration
    config = load_config('config/config.yaml')
    
    # Create export specification
    export_spec = TimeRangeExport(
        start_time=datetime(2025, 12, 10, 0, 0, 0),
        end_time=datetime(2025, 12, 11, 0, 0, 0),
        tables=["empatica", "blueiot"],
        format="parquet",
        include_ticket_id=True
    )
    
    # Create exporter
    export_config = ExportConfig(
        output_dir=Path("./exports"),
        parquet_compression="snappy"
    )
    exporter = BigQueryExporter(config, export_config)
    
    # Execute export
    print(f"\nExporting data from {export_spec.start_time} to {export_spec.end_time}...")
    output_files = await exporter.export_time_range(export_spec)
    
    print("\n✓ Export completed!")
    print("\nOutput files:")
    for table, path in output_files.items():
        size = path.stat().st_size / (1024 * 1024)  # MB
        print(f"  - {table}: {path} ({size:.2f} MB)")
    
    return output_files


async def example_export_ticket():
    """Example: Export data for a specific ticket (Avro format for replay)."""
    print("\n" + "=" * 80)
    print("Example 3: Export Ticket (Avro for Redis Replay)")
    print("=" * 80)
    
    # Load configuration
    config = load_config('config/config.yaml')
    
    # First, get a ticket ID
    exporter = BigQueryExporter(config)
    tickets = await exporter.list_available_tickets(
        start_time="2025-12-01T00:00:00",
        end_time="2025-12-10T23:59:59"
    )
    
    if not tickets:
        print("No tickets found. Skipping this example.")
        return {}
    
    ticket_id = tickets[0]['ticket_id']
    print(f"\nExporting ticket: {ticket_id}")
    
    # Create export specification
    export_spec = TicketExport(
        ticket_id=ticket_id,
        tables=["empatica", "blueiot", "global_state_events"],
        format="avro"
    )
    
    # Create exporter
    export_config = ExportConfig(
        output_dir=Path("./exports"),
        avro_codec="deflate"
    )
    exporter = BigQueryExporter(config, export_config)
    
    # Execute export
    output_files = await exporter.export_by_ticket(export_spec)
    
    print("\n✓ Export completed!")
    print("\nOutput files:")
    for table, path in output_files.items():
        size = path.stat().st_size / (1024 * 1024)  # MB
        print(f"  - {table}: {path} ({size:.2f} MB)")
    
    return output_files


def example_analyze_parquet(parquet_file: Path):
    """Example: Analyze exported Parquet data with pandas."""
    print("\n" + "=" * 80)
    print("Example 4: Analyze Parquet Data with Pandas")
    print("=" * 80)
    
    if not parquet_file.exists():
        print(f"File not found: {parquet_file}")
        return
    
    # Load Parquet file
    print(f"\nLoading: {parquet_file}")
    df = pd.read_parquet(parquet_file)
    
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Show basic statistics
    print("\n--- Basic Statistics ---")
    print(df.describe())
    
    # Check for specific columns
    if 'heart_rate' in df.columns:
        print("\n--- Heart Rate Analysis ---")
        print(f"Mean HR: {df['heart_rate'].mean():.2f} bpm")
        print(f"Min HR: {df['heart_rate'].min():.2f} bpm")
        print(f"Max HR: {df['heart_rate'].max():.2f} bpm")
        
        # Time series analysis
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
        df.set_index('event_timestamp', inplace=True)
        
        # Resample to 1-minute averages
        hr_1min = df['heart_rate'].resample('1min').mean()
        
        print(f"\nTime range: {df.index.min()} to {df.index.max()}")
        print(f"Duration: {df.index.max() - df.index.min()}")
        
        # Plot (if in interactive environment)
        try:
            plt.figure(figsize=(12, 6))
            hr_1min.plot()
            plt.title('Heart Rate Over Time (1-min averages)')
            plt.ylabel('Heart Rate (bpm)')
            plt.xlabel('Time')
            plt.grid(True)
            plt.savefig('./exports/heart_rate_plot.png')
            print("\n✓ Plot saved to: ./exports/heart_rate_plot.png")
        except Exception as e:
            print(f"\nCouldn't create plot: {e}")
    
    # Show first few rows
    print("\n--- Sample Data (first 5 rows) ---")
    print(df.head())


def example_read_avro(avro_file: Path):
    """Example: Read and display Avro replay format."""
    print("\n" + "=" * 80)
    print("Example 5: Read Avro Replay Format")
    print("=" * 80)
    
    if not avro_file.exists():
        print(f"File not found: {avro_file}")
        return
    
    try:
        from fastavro import reader
        
        print(f"\nReading: {avro_file}\n")
        
        with open(avro_file, 'rb') as f:
            avro_reader = reader(f)
            
            # Read first 10 records
            for i, record in enumerate(avro_reader):
                if i >= 10:
                    break
                
                timestamp = datetime.fromtimestamp(record['timestamp'] / 1000)
                event_type = record['type']
                key = record['key']
                value = record['value'][:50] + '...' if len(record['value']) > 50 else record['value']
                
                print(f"[{timestamp}] {event_type:8} | {key:40} | {value}")
        
        print("\n... (showing first 10 events)")
        
    except ImportError:
        print("fastavro not installed. Run: pip install fastavro")


async def main():
    """Run all examples."""
    try:
        # Example 1: List tickets
        tickets = await example_list_tickets()
        
        # Example 2: Export time range (Parquet)
        output_files = await example_export_timerange()
        
        # Example 3: Export ticket (Avro)
        avro_files = await example_export_ticket()
        
        # Example 4: Analyze Parquet data
        if output_files and 'empatica' in output_files:
            example_analyze_parquet(output_files['empatica'])
        
        # Example 5: Read Avro replay format
        if avro_files and 'empatica' in avro_files:
            example_read_avro(avro_files['empatica'])
        
        print("\n" + "=" * 80)
        print("All examples completed!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    # Run examples
    asyncio.run(main())
