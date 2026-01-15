"""SQL query generation for BigQuery exports."""

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class ExportQueryBuilder:
    """Generates optimized BigQuery SQL queries for data export."""
    
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.full_dataset = f"{project_id}.{dataset_id}"
    
    def build_time_range_query(
        self,
        table: str,
        start_time: datetime,
        end_time: datetime,
        include_ticket_id: bool = True
    ) -> str:
        """
        Generate query to export data within a time range.
        
        For empatica and blueiot tables, optionally enriches with ticket_id
        by joining with global_state_events to resolve device_id → ticket_id mappings.
        
        Args:
            table: Table name (empatica, blueiot, or global_state_events)
            start_time: Start of time range (inclusive)
            end_time: End of time range (exclusive)
            include_ticket_id: Whether to enrich with ticket_id (only for empatica/blueiot)
        
        Returns:
            SQL query string
        """
        if table == "global_state_events":
            # Simple query for global_state_events
            return self._build_global_state_events_query(start_time, end_time)
        
        elif table in ("empatica", "blueiot"):
            if include_ticket_id:
                return self._build_biometric_query_with_ticket_id(
                    table, start_time, end_time
                )
            else:
                return self._build_simple_biometric_query(
                    table, start_time, end_time
                )
        
        else:
            raise ValueError(f"Unknown table: {table}")
    
    def build_ticket_query(
        self,
        table: str,
        ticket_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> str:
        """
        Generate query to export all data for a specific ticket_id.
        
        For empatica and blueiot, this resolves device_id(s) associated with
        the ticket_id and queries based on those device_ids.
        
        Args:
            table: Table name
            ticket_id: Ticket ID to export
            start_time: Optional start time filter
            end_time: Optional end time filter
        
        Returns:
            SQL query string
        """
        if table == "global_state_events":
            return self._build_global_state_events_ticket_query(
                ticket_id, start_time, end_time
            )
        
        elif table in ("empatica", "blueiot"):
            return self._build_biometric_ticket_query(
                table, ticket_id, start_time, end_time
            )
        
        else:
            raise ValueError(f"Unknown table: {table}")
    
    def _build_global_state_events_query(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> str:
        """Build query for global_state_events table."""
        return f"""
SELECT
    event_timestamp,
    state_key,
    state_value,
    ttl_seconds
FROM `{self.full_dataset}.global_state_events`
WHERE event_timestamp >= TIMESTAMP('{start_time.isoformat()}')
  AND event_timestamp < TIMESTAMP('{end_time.isoformat()}')
ORDER BY event_timestamp ASC
""".strip()
    
    def _build_simple_biometric_query(
        self,
        table: str,
        start_time: datetime,
        end_time: datetime
    ) -> str:
        """Build simple query for biometric tables without ticket_id enrichment."""
        return f"""
SELECT *
FROM `{self.full_dataset}.{table}`
WHERE event_timestamp >= TIMESTAMP('{start_time.isoformat()}')
  AND event_timestamp < TIMESTAMP('{end_time.isoformat()}')
ORDER BY event_timestamp ASC
""".strip()
    
    def _build_biometric_query_with_ticket_id(
        self,
        table: str,
        start_time: datetime,
        end_time: datetime
    ) -> str:
        """
        Build query for biometric tables with ticket_id enrichment.
        
        This joins with global_state_events to map device_id → ticket_id.
        Handles device reassignments by using time-based windows.
        """
        return f"""
WITH device_mappings AS (
  -- Extract device_id → ticket_id mappings from EmpaticaDeviceID events
  SELECT
    REGEXP_EXTRACT(state_key, r'Visitors:([^:]+):EmpaticaDeviceID') as ticket_id,
    state_value as device_id,
    event_timestamp as assigned_at,
    -- Use LEAD to find when this device was reassigned
    LEAD(event_timestamp) OVER (
      PARTITION BY state_value
      ORDER BY event_timestamp
    ) as valid_until
  FROM `{self.full_dataset}.global_state_events`
  WHERE state_key LIKE 'Visitors:%:EmpaticaDeviceID'
    AND event_timestamp >= TIMESTAMP('{start_time.isoformat()}')
    AND event_timestamp < TIMESTAMP('{end_time.isoformat()}')
)
SELECT
  data.*,
  COALESCE(dm.ticket_id, 'UNKNOWN') as ticket_id
FROM `{self.full_dataset}.{table}` data
LEFT JOIN device_mappings dm
  ON data.device_id = dm.device_id
  -- Join on time window: data timestamp must fall within device assignment period
  AND data.event_timestamp >= dm.assigned_at
  AND (data.event_timestamp < dm.valid_until OR dm.valid_until IS NULL)
WHERE data.event_timestamp >= TIMESTAMP('{start_time.isoformat()}')
  AND data.event_timestamp < TIMESTAMP('{end_time.isoformat()}')
ORDER BY data.event_timestamp ASC
""".strip()
    
    def _build_global_state_events_ticket_query(
        self,
        ticket_id: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> str:
        """Build query to get global_state_events for a specific ticket."""
        time_filter = ""
        if start_time:
            time_filter += f"\n  AND event_timestamp >= TIMESTAMP('{start_time.isoformat()}')"
        if end_time:
            time_filter += f"\n  AND event_timestamp < TIMESTAMP('{end_time.isoformat()}')"
        
        return f"""
SELECT
    event_timestamp,
    state_key,
    state_value,
    ttl_seconds
FROM `{self.full_dataset}.global_state_events`
WHERE state_key LIKE 'Visitors:{ticket_id}:%'{time_filter}
ORDER BY event_timestamp ASC
""".strip()
    
    def _build_biometric_ticket_query(
        self,
        table: str,
        ticket_id: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> str:
        """
        Build query to get biometric data for a specific ticket.
        
        First resolves device_id(s) associated with the ticket,
        then queries biometric data for those devices.
        """
        data_time_filter = ""
        if start_time:
            data_time_filter += f"\n  AND data.event_timestamp >= TIMESTAMP('{start_time.isoformat()}')"
        if end_time:
            data_time_filter += f"\n  AND data.event_timestamp < TIMESTAMP('{end_time.isoformat()}')"
        
        return f"""
WITH ticket_devices AS (
  -- Find all device_ids ever associated with this ticket
  -- Note: We do NOT apply time filters here, as device assignments
  -- may have occurred before the requested time range
  SELECT DISTINCT
    state_value as device_id
  FROM `{self.full_dataset}.global_state_events`
  WHERE state_key = 'Visitors:{ticket_id}:EmpaticaDeviceID'
    AND state_value IS NOT NULL
    AND state_value != ''
)
SELECT
  data.*,
  '{ticket_id}' as ticket_id
FROM `{self.full_dataset}.{table}` data
INNER JOIN ticket_devices td
  ON data.device_id = td.device_id
WHERE 1=1{data_time_filter}
ORDER BY data.event_timestamp ASC
""".strip()