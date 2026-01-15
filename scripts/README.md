# Scripts

Utility scripts for the RDC-BigQuery bridge project.

## Available Scripts

### `populate_sample_data.py`

Populates BigQuery with synthetic sample data for testing the export functionality.

**Purpose**: Generate realistic test data across all three tables (`empatica`, `blueiot`, `global_state_events`) to validate exports and analysis.

**Features**:
- Generates configurable number of tickets and devices
- Creates realistic biosensor data (heart rate, accelerometer, etc.)
- Simulates position tracking with multiple anchors
- Establishes ticket-device relationships in global_state_events
- Generates data over configurable time periods

**Usage**:

```bash
# Basic usage (10 tickets, 7 days)
python scripts/populate_sample_data.py

# Custom configuration
python scripts/populate_sample_data.py --tickets 20 --days 14

# With custom config file
python scripts/populate_sample_data.py --config /path/to/config.yaml --tickets 50 --days 30
```

**Options**:
- `--config`: Path to config file (default: config/config.yaml)
- `--tickets`: Number of tickets to generate (default: 10)
- `--days`: Number of days of historical data (default: 7)

**Output**:

```
2025-12-10 16:40:00 - INFO - Generating empatica data for 10 devices over 7 days...
2025-12-10 16:40:05 - INFO - Inserted 1000 empatica rows
2025-12-10 16:40:10 - INFO - Inserted 1000 empatica rows
...
2025-12-10 16:40:45 - INFO - ✓ Empatica data population complete
2025-12-10 16:40:45 - INFO - Generating blueiot data for 10 devices over 7 days...
2025-12-10 16:41:15 - INFO - ✓ BlueIoT data population complete
2025-12-10 16:41:15 - INFO - Generating global_state_events for 10 tickets...
2025-12-10 16:41:20 - INFO - ✓ Global state events population complete

================================================================================
✓ Sample data population complete!
================================================================================

Summary:
  Tickets: 10
  Devices: 10
  Days: 7

Sample ticket IDs: TICKET001, TICKET002, TICKET003, TICKET004, TICKET005
Sample device IDs: DEVICE_AA_01, DEVICE_AA_02, DEVICE_AA_03, DEVICE_AA_04, DEVICE_AA_05

You can now test exports:
  rdc-export list-tickets --start "2025-12-03" --end "2025-12-10"
  rdc-export export-ticket --ticket-id TICKET001 --format parquet
```

**Generated Data Structure**:

1. **empatica table** (~40,000 rows per 10 tickets, 7 days)
   - `event_timestamp`: Random times during "active" hours
   - `device_id`: Assigned device ID
   - `heart_rate`: Realistic values (60-100 bpm with variation)
   - `skin_conductivity`: Galvanic skin response values
   - `skin_temperature`: Body temperature
   - `accelerometer`: Random acceleration values
   - `engagement`: Engagement scores (0-1)
   - `excitement`: Excitement level (0-1)
   - `stress`: Stress level (0-1)
   - `battery_level`: Battery percentage (20-100%)

2. **blueiot table** (~40,000 rows per 10 tickets, 7 days)
   - `event_timestamp`: Matching empatica timestamps
   - `device_id`: Same device IDs
   - `position_x`: X coordinate (0-100)
   - `position_y`: Y coordinate (0-100)
   - `position_z`: Z coordinate (0-5)
   - `zone`: Zone name (Zone_A, Zone_B, Lobby, etc.)

3. **global_state_events table** (~200 rows per 10 tickets, 7 days)
   - Device assignment events: `Visitors:TICKET001:EmpaticaDeviceID`
   - Status events: `Visitors:TICKET001:Status`
   - Random status changes throughout visit

**Test Workflow**:

After populating data:

```bash
# 1. List available tickets
rdc-export list-tickets --start "2025-12-03" --end "2025-12-10"

# 2. Export first ticket (Parquet)
rdc-export export-ticket --ticket-id TICKET001 --format parquet

# 3. Export time range (Avro for replay)
rdc-export export-timerange \
  --start "2025-12-03T00:00:00" \
  --end "2025-12-04T00:00:00" \
  --format avro

# 4. Analyze in Python
python examples/export_and_analyze.py
```

**Cleaning Up**:

To remove test data:

```bash
# Delete all data from tables
bq query --use_legacy_sql=false \
  "DELETE FROM \`your-project.rdc_bridge.empatica\` WHERE TRUE"

bq query --use_legacy_sql=false \
  "DELETE FROM \`your-project.rdc_bridge.blueiot\` WHERE TRUE"

bq query --use_legacy_sql=false \
  "DELETE FROM \`your-project.rdc_bridge.global_state_events\` WHERE TRUE"
```

### `monitor_redis.py`

Monitors Redis channels and keyspace notifications based on configured routing rules.

**Usage**:

```bash
python scripts/monitor_redis.py
```

See project README for more details.

## Development

### Adding New Scripts

When adding new scripts to this directory:

1. Add a clear docstring at the top
2. Include usage examples in the docstring
3. Add argparse for command-line arguments
4. Update this README with script details
5. Make scripts executable: `chmod +x scripts/your_script.py`

### Testing Scripts

Test scripts with different configurations:

```bash
# Test with minimal data
python scripts/populate_sample_data.py --tickets 2 --days 1

# Test with realistic data
python scripts/populate_sample_data.py --tickets 100 --days 30
```

## Troubleshooting

### "Permission denied"

Ensure BigQuery permissions:
- BigQuery Data Editor
- BigQuery Job User

Check credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### "Table not found"

Ensure tables exist:
```bash
python -m src.main  # Run main app to create tables
# Or manually create tables via BigQuery console
```

### "Out of memory"

Reduce batch size or number of tickets:
```bash
python scripts/populate_sample_data.py --tickets 5 --days 3
```

## See Also

- [Main README](../README.md) - Project overview
- [EXPORT_GUIDE.md](../EXPORT_GUIDE.md) - Export documentation
- [examples/](../examples/) - Usage examples