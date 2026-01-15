# Real-Time RDC-to-BigQuery Data Bridge

A high-performance, resilient Python application that synchronizes data from Redis Data Cache (RDC) into Google BigQuery in near real-time. Built with asyncio for handling high-velocity data from ~800 concurrent visitors with an estimated throughput of 1,000 events per second.

## Features

- **High Performance**: Fully asynchronous architecture using Python's asyncio
- **Scalable**: Handles 800+ concurrent users with horizontal scaling support
- **Resilient**: Robust error handling with automatic retry mechanisms
- **Config-Driven**: All schemas, routing, and business logic controlled via YAML configuration
- **Efficient**: Uses Google BigQuery Storage Write API for high-throughput ingestion
- **No Hardcoded Schemas**: Table schemas, partitioning, and clustering fully defined in config.yaml
- **Data Export**: Export BigQuery data in Parquet (for data scientists) or Avro (for Redis replay) formats

## Architecture

The application implements a Streaming ETL pipeline with three main components:

1. **Redis Ingestor**: Listens to Redis invalidation messages (via client-side tracking) and channel subscriptions
2. **Row Processor**: Processes incoming Redis events and transforms data for BigQuery
3. **BigQuery Loader**: Batches and loads data into dedicated BigQuery tables using the Storage Write API

### Data Flow

```
Redis (Invalidation Messages/Channels) 
  → Redis Ingestor 
  → Row Assembler (applies routing rules)
  → Row Processor (transforms data)
  → BigQuery Loader 
  → BigQuery Tables (empatica, blueiot, global_state_events)
```

## Prerequisites

### Google Cloud Platform Setup

1. **GCP Project & APIs**:
   - Create a GCP project
   - Enable BigQuery API and BigQuery Storage Write API

2. **Service Account**:
   - Create a service account with roles: `BigQuery Data Editor` and `BigQuery Job User`
   - Download the JSON key file

3. **BigQuery Tables**:
   - Dataset and tables are automatically created on first run
   - Schemas are defined in `config.yaml`
   - Tables: `empatica` (biosensor data), `blueiot` (position data), `global_state_events` (state changes)

### Redis Configuration

**No manual Redis configuration is required!** The application automatically enables client-side tracking when it starts.

Client-side tracking uses Redis's `CLIENT TRACKING` feature with BCAST mode to receive invalidation messages only when keys actually change, not on every SET operation. This is more efficient than keyspace notifications and reduces duplicate event processing.

**Requirements:**
- Redis 6.0 or newer (for client-side tracking support)

### Python Environment

- Python 3.11 or newer
- Poetry for dependency management

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd dataland-rdc-bq-bridge
```

2. Install dependencies:
```bash
poetry install
```

3. Configure the application:
```bash
cp config/config.yaml.example config/config.yaml
# Edit config.yaml with your settings
```

## Configuration

The main configuration file is `config/config.yaml`. All schemas, routing rules, and infrastructure settings are defined here.

### GCP Configuration
```yaml
gcp:
  project_id: "your-gcp-project-id"
  dataset_id: "rdc_bridge"
  credentials_file: "/path/to/service-account.json"
```

### Redis Configuration
```yaml
redis:
  host: "localhost"
  port: 6379
  password: "your-password"  # Optional
```

### Pipeline Routing Rules
Define how Redis data is routed to BigQuery tables:

```yaml
pipeline:
  routing_rules:
    - name: "BioSensors"
      redis_source_type: "channel"
      redis_pattern: "Wearables:WatchDevices:*:BioSensors"
      target_table: "empatica"
      id_parser_regex: "Wearables:WatchDevices:(?P<device_id>[^:]+):BioSensors"
      value_is_object: true

    - name: "Position"
      redis_source_type: "channel"
      redis_pattern: "Wearables:WatchDevices:*:BlueIoTSensors"
      target_table: "blueiot"
      id_parser_regex: "Wearables:WatchDevices:(?P<device_id>[^:]+):BlueIoTSensors"
      value_is_object: true

    - name: "GlobalState"
      redis_source_type: "key"
      redis_pattern: "Visitors:*"
      target_table: "global_state_events"
      value_is_object: false
```

### BigQuery Table Schemas
Define table schemas, partitioning, clustering, and field mappings:

```yaml
bigquery:
  tables:
    empatica:
      name: "empatica"
      description: "Raw telemetry from Empatica wearable devices"
      partitioning_field: "event_timestamp"
      partitioning_type: "DAY"
      clustering_fields:
        - "device_id"
      schema:
        - name: "event_timestamp"
          type: "TIMESTAMP"
          mode: "REQUIRED"
        - name: "device_id"
          type: "STRING"
          mode: "REQUIRED"
        - name: "heart_rate"
          type: "FLOAT64"
          mode: "NULLABLE"
        # ... additional fields
      field_mappings:
        Heart_Rate_Value: "heart_rate"
        Accelerometer_Value: "accelerometer"
        # Maps Redis field names to BigQuery column names
```

### Loader Configuration
```yaml
loader:
  batch_size_rows: 500
  batch_size_bytes: 1000000
  commit_interval_seconds: 1.0
```

## Usage

### Running the Bridge Application

```bash
# Using poetry
poetry run rdc-bridge

# Or directly with Python
python -m src.main

# With custom config file
RDC_BRIDGE_CONFIG=/path/to/config.yaml poetry run rdc-bridge
```

### Data Export

Export data from BigQuery for analysis or Redis replay:

```bash
# Export by time range (Parquet for data scientists)
rdc-export export-timerange \
  --start "2025-12-01T00:00:00" \
  --end "2025-12-10T23:59:59" \
  --format parquet

# Export by ticket (Avro for Redis replay - creates single combined file)
rdc-export export-ticket \
  --ticket-id TICKET123 \
  --format avro

# List available tickets
rdc-export list-tickets --start "2025-12-01" --end "2025-12-10"
```

**Export Formats:**
- **Parquet**: Columnar format optimized for data science workflows (pandas, duckdb, Jupyter). Creates separate files per table.
- **Avro**: Row-oriented format optimized for sequential Redis event replay. **Combines all tables into a single file ordered by timestamp** for accurate replay.
- **JSONL**: Human-readable format for debugging. Creates separate files per table.

See [EXPORT_GUIDE.md](EXPORT_GUIDE.md) for complete export documentation.

### Redis Monitoring Utility

Monitor Redis channels and keyspace notifications based on configured routing rules:

```bash
python scripts/monitor_redis.py
```

### Generate Synthetic Test Data

Populate BigQuery with synthetic sample data for testing:

```bash
python scripts/generate_synthetic_data.py
```

## Environment Variables

- `RDC_BRIDGE_CONFIG`: Path to configuration file (default: `config/config.yaml`)
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to GCP service account key file

## Development

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black src/ tests/
poetry run isort src/ tests/
```

### Type Checking

```bash
poetry run mypy src/
```

## Monitoring

The application provides built-in monitoring and logging:

- Queue depth monitoring for backpressure detection
- BigQuery commit latency and throughput metrics
- Error count tracking by type
- Component health monitoring

Logs are written to both stdout and `rdc_bridge.log`.

## Performance Tuning

### Batch Configuration
```yaml
loader:
  batch_size_rows: 500      # Rows per batch
  batch_size_bytes: 1000000 # Bytes per batch
  commit_interval_seconds: 1.0  # Max time between commits
```

### Key Configuration Features

- **No Hardcoded Schemas**: All table schemas defined in config.yaml
- **Field Mappings**: Map Redis field names to BigQuery columns in config
- **Flexible Routing**: Route Redis patterns to specific tables via regex
- **Dynamic Schema**: Add/remove fields by updating config.yaml only

## Deployment

### Docker

```bash
# Build image
docker build -t rdc-bridge .

# Run container
docker run -d \
  -v /path/to/config.yaml:/app/config/config.yaml \
  -v /path/to/service-account.json:/app/credentials.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
  rdc-bridge
```

### Kubernetes

See `k8s/` directory for Kubernetes deployment manifests.

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**: Check Redis URL and network connectivity
2. **BigQuery Permission Denied**: Verify service account has required roles
3. **High Queue Depth**: Increase batch sizes or check BigQuery quotas
4. **Missing Data**: Verify Redis version supports client-side tracking (Redis 6.0+)

### Debug Mode

Enable debug logging:
```python
logging.getLogger().setLevel(logging.DEBUG)
```

Or use the `--debug` flag with export commands:
```bash
rdc-export export-timerange --start "..." --end "..." --debug
```

## Project Structure

```
dataland-rdc-bq-bridge/
├── src/
│   ├── main.py                 # Application entry point
│   ├── config.py               # Configuration loader
│   ├── ingestor/               # Redis ingestion layer
│   │   ├── redis_ingestor.py
│   │   └── row_assembler.py
│   ├── processor/              # Data transformation layer
│   │   └── row_processor.py
│   ├── loader/                 # BigQuery loading layer
│   │   └── bigquery_loader.py
│   └── exporter/               # Data export functionality
│       ├── cli.py
│       ├── bq_exporter.py
│       ├── query_builder.py
│       ├── format_writer.py
│       └── export_config.py
├── config/
│   └── config.yaml             # Main configuration file
├── scripts/
│   ├── monitor_redis.py        # Redis monitoring utility
│   └── generate_synthetic_data.py  # Test data generator
├── tests/                      # Unit and integration tests
├── examples/                   # Usage examples
├── EXPORT_GUIDE.md            # Detailed export documentation
└── README.md                  # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run linting and tests
5. Submit a pull request

## License

[Your License Here]

## Support

For issues and questions, please create an issue in the repository.