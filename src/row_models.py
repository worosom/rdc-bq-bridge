"""Data models for row assembly in the RDC-BigQuery bridge."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set


@dataclass
class RowInProgress:
    """Represents a row being processed."""
    row_id: str
    target_table: str
    data: Dict[str, Any] = field(default_factory=dict)
    last_update: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    # Track which data types we've received (useful if we were waiting for multiple sources,
    # but in the simplified pipeline we might just use this for logging)
    received_data_types: Set[str] = field(default_factory=set)


class AssembledRow:
    """Represents a completed row ready for BigQuery insertion."""

    def __init__(self, target_table: str, data: Dict[str, Any]):
        self.target_table = target_table
        self.event_timestamp = datetime.now(timezone.utc)
        # Include event_timestamp in the data dict for BigQuery
        self.data = {**data, 'event_timestamp': self.event_timestamp}