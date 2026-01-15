"""Configuration models for BigQuery exports."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional


ExportFormat = Literal["parquet", "avro", "jsonl"]
ExportType = Literal["timerange", "ticket"]


@dataclass
class ExportConfig:
    """Base export configuration."""
    
    # Output settings
    output_dir: Path = Path("./exports")
    temp_dir: Path = Path("/tmp/bq_exports")
    
    # Format settings
    default_format: ExportFormat = "parquet"
    
    # Parquet settings
    parquet_compression: str = "snappy"  # Options: snappy, gzip, zstd
    parquet_row_group_size: int = 100000
    
    # Avro settings
    avro_codec: str = "deflate"  # Options: null, deflate, snappy
    
    # Query limits
    max_rows_per_export: int = 10_000_000  # 10M rows
    chunk_size: int = 1_000_000  # 1M rows per chunk
    
    # BigQuery export job settings
    use_gcs_staging: bool = False  # Use GCS for large exports
    gcs_staging_bucket: Optional[str] = None
    gcs_staging_path: str = "bq-exports/staging/"
    
    def __post_init__(self) -> None:
        """Ensure paths are Path objects."""
        if isinstance(self.output_dir, str):
            self.output_dir = Path(self.output_dir)
        if isinstance(self.temp_dir, str):
            self.temp_dir = Path(self.temp_dir)
        
        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class TimeRangeExport:
    """Configuration for time-range based export."""
    
    start_time: datetime
    end_time: datetime
    tables: list[str] = field(default_factory=lambda: ["empatica", "blueiot", "global_state_events"])
    format: ExportFormat = "parquet"
    output_path: Optional[Path] = None
    include_ticket_id: bool = True  # Enrich biometric data with ticket_id
    
    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.start_time >= self.end_time:
            raise ValueError("start_time must be before end_time")
        
        valid_tables = {"empatica", "blueiot", "global_state_events"}
        invalid_tables = set(self.tables) - valid_tables
        if invalid_tables:
            raise ValueError(f"Invalid tables: {invalid_tables}. Valid tables: {valid_tables}")
        
        if isinstance(self.output_path, str):
            self.output_path = Path(self.output_path)


@dataclass
class TicketExport:
    """Configuration for ticket-based export."""
    
    ticket_id: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    tables: list[str] = field(default_factory=lambda: ["empatica", "blueiot", "global_state_events"])
    format: ExportFormat = "parquet"
    output_path: Optional[Path] = None
    
    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise ValueError("start_time must be before end_time")
        
        valid_tables = {"empatica", "blueiot", "global_state_events"}
        invalid_tables = set(self.tables) - valid_tables
        if invalid_tables:
            raise ValueError(f"Invalid tables: {invalid_tables}. Valid tables: {valid_tables}")
        
        if isinstance(self.output_path, str):
            self.output_path = Path(self.output_path)
