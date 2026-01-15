# Export Examples

This directory contains example scripts demonstrating how to use the BigQuery export functionality.

## Files

### `export_and_analyze.py`

Comprehensive example script that demonstrates:

1. **Listing available tickets** - Find all tickets in a time range
2. **Time range export (Parquet)** - Export data for data scientists
3. **Ticket export (Avro)** - Export data for Redis replay
4. **Analyzing Parquet data** - Load and analyze with pandas
5. **Reading Avro replay format** - Inspect Redis event stream format

## Running the Examples

### Prerequisites

```bash
# Install dependencies
poetry install

# Ensure you have valid GCP credentials configured
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### Run All Examples

```bash
cd /home/alex/dataland-rdc-bq-bridge
python examples/export_and_analyze.py
```

### Expected Output

```
================================================================================
Example 1: List Available Tickets
================================================================================

Found 42 tickets:

Ticket ID            First Seen                Last Seen                 Events
-------------------------------------------------------------------------------------
TICKET123            2025-12-01 09:00:00      2025-12-01 17:30:00       1543
TICKET124            2025-12-01 09:15:00      2025-12-01 18:00:00       1892
...

================================================================================
Example 2: Export Time Range (Parquet for Data Scientists)
================================================================================

Exporting data from 2025-12-01 00:00:00 to 2025-12-02 00:00:00...

✓ Export completed!

Output files:
  - empatica: ./exports/timerange_20251201_000000_to_20251202_000000/empatica.parquet (2.34 MB)
  - blueiot: ./exports/timerange_20251201_000000_to_20251202_000000/blueiot.parquet (1.87 MB)

================================================================================
Example 3: Export Ticket (Avro for Redis Replay)
================================================================================

Exporting ticket: TICKET123

✓ Export completed!

Output files:
  - empatica: ./exports/ticket_TICKET123/empatica.avro (1.56 MB)
  - blueiot: ./exports/ticket_TICKET123/blueiot.avro (1.23 MB)
  - global_state_events: ./exports/ticket_TICKET123/global_state_events.avro (0.45 MB)

================================================================================
Example 4: Analyze Parquet Data with Pandas
================================================================================

Loading: ./exports/timerange_20251201_000000_to_20251202_000000/empatica.parquet

DataFrame shape: (15432, 9)
Columns: ['event_timestamp', 'device_id', 'heart_rate', 'accelerometer', 'engagement', 'steps', 'gsr', 'temperature', 'ticket_id']

--- Basic Statistics ---
       heart_rate  accelerometer  engagement
count   15432.000      15432.000   15432.000
mean       72.345          1.234       0.567
std        12.456          0.345       0.123
...

--- Heart Rate Analysis ---
Mean HR: 72.35 bpm
Min HR: 45.00 bpm
Max HR: 145.00 bpm

Time range: 2025-12-01 00:00:15 to 2025-12-01 23:59:45
Duration: 0 days 23:59:30

✓ Plot saved to: ./exports/heart_rate_plot.png

--- Sample Data (first 5 rows) ---
                           device_id  heart_rate  accelerometer  engagement  ticket_id
event_timestamp                                                                        
2025-12-01 00:00:15    DEVICE_ABC_01       68.5          0.234       0.567  TICKET123
2025-12-01 00:00:30    DEVICE_ABC_01       69.2          0.245       0.578  TICKET123
...

================================================================================
Example 5: Read Avro Replay Format
================================================================================

Reading: ./exports/ticket_TICKET123/empatica.avro

[2025-12-01 09:00:15] channel  | Wearables:WatchDevices:DEVICE_ABC_01:BioSensors | {"Heart_Rate_Value": 68.5, "Accelerometer_Valu...
[2025-12-01 09:00:30] channel  | Wearables:WatchDevices:DEVICE_ABC_01:BioSensors | {"Heart_Rate_Value": 69.2, "Accelerometer_Valu...
[2025-12-01 09:00:45] channel  | Wearables:WatchDevices:DEVICE_ABC_01:BioSensors | {"Heart_Rate_Value": 70.1, "Accelerometer_Valu...
...

... (showing first 10 events)

================================================================================
All examples completed!
================================================================================
```

## Customizing the Examples

### Change Time Range

Edit `example_export_timerange()`:

```python
export_spec = TimeRangeExport(
    start_time=datetime(2025, 12, 1, 0, 0, 0),  # Change start
    end_time=datetime(2025, 12, 10, 23, 59, 59),  # Change end
    tables=["empatica", "blueiot"],
    format="parquet",
    include_ticket_id=True
)
```

### Export Specific Ticket

Edit `example_export_ticket()`:

```python
# Instead of getting first ticket, use a specific one
ticket_id = "TICKET123"  # Your ticket ID

export_spec = TicketExport(
    ticket_id=ticket_id,
    tables=["empatica", "blueiot", "global_state_events"],
    format="avro"
)
```

### Change Output Format

```python
# For Parquet (data science)
format="parquet"

# For Avro (Redis replay)
format="avro"

# For JSONL (debugging)
format="jsonl"
```

### Change Compression

```python
export_config = ExportConfig(
    output_dir=Path("./exports"),
    parquet_compression="gzip",  # Options: snappy, gzip, zstd
    avro_codec="snappy"  # Options: null, deflate, snappy
)
```

## Next Steps

After running the examples:

1. **Explore the exported files**:
   ```bash
   ls -lh ./exports/
   ```

2. **Open in Jupyter**:
   ```python
   import pandas as pd
   df = pd.read_parquet('./exports/timerange_*/empatica.parquet')
   df.head()
   ```

3. **Use the CLI directly**:
   ```bash
   rdc-export list-tickets --start "2025-12-01" --end "2025-12-10"
   rdc-export export-ticket --ticket-id TICKET123 --format parquet
   ```

4. **Build your own analysis**:
   - Copy `export_and_analyze.py` as a template
   - Modify for your specific analysis needs
   - Add custom visualizations
   - Export results to reports

## Troubleshooting

### "No module named 'src'"

Run from the project root:
```bash
cd /home/alex/dataland-rdc-bq-bridge
python examples/export_and_analyze.py
```

Or install the package:
```bash
poetry install
python examples/export_and_analyze.py
```

### "Configuration file not found"

Ensure `config/config.yaml` exists with valid GCP credentials.

### "No tickets found"

- Check your time range includes data
- Verify BigQuery tables contain data
- Check GCP permissions

## See Also

- [EXPORT_GUIDE.md](../EXPORT_GUIDE.md) - Complete export documentation
- [EXPORT_QUICK_REFERENCE.md](../EXPORT_QUICK_REFERENCE.md) - Quick command reference
- [README.md](../README.md) - Project overview
