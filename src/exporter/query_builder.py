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
        end_time: Optional[datetime] = None,
        ticket_timeframe: Optional[tuple] = None
    ) -> str:
        """
        Generate query to export all data for a specific ticket_id.
        
        For empatica and blueiot, this filters by ticket_id directly.
        For global_state_events, when ticket_timeframe is provided, this exports
        ALL events within the ticket's timeframe (not just the ticket's own events)
        to capture complete system state during the ticket's session.
        
        Args:
            table: Table name
            ticket_id: Ticket ID to export
            start_time: Optional start time filter
            end_time: Optional end time filter
            ticket_timeframe: Optional tuple of (first_seen, last_seen) timestamps
                            for global_state_events time range query
        
        Returns:
            SQL query string
        """
        if table == "global_state_events":
            if ticket_timeframe:
                # Export ALL events in ticket timeframe, not just ticket's own events
                first_seen, last_seen = ticket_timeframe
                return self._build_global_state_events_timeframe_query(
                    first_seen, last_seen, start_time, end_time
                )
            else:
                # Fallback: export only ticket's own events
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
        Build query for biometric tables with ticket_id.
        
        With the new schema, ticket_id is stored directly in the table,
        so no complex joins are needed anymore!
        """
        return f"""
SELECT *
FROM `{self.full_dataset}.{table}`
WHERE event_timestamp >= TIMESTAMP('{start_time.isoformat()}')
  AND event_timestamp < TIMESTAMP('{end_time.isoformat()}')
ORDER BY event_timestamp ASC
""".strip()
    
    def _build_global_state_events_timeframe_query(
        self,
        first_seen,
        last_seen,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> str:
        """
        Build query to get ALL global_state_events within ticket's timeframe.
        
        This exports all events (from any ticket) that occurred during the ticket's
        active period, providing complete system state context for Redis replay.
        
        User-provided start/end times further restrict the range if specified.
        """
        # Use ticket timeframe as base
        query_start = first_seen
        query_end = last_seen
        
        # Apply user-provided time filters if they narrow the range
        if start_time and start_time > query_start:
            query_start = start_time
        if end_time and end_time < query_end:
            query_end = end_time
        
        return f"""
SELECT
    event_timestamp,
    ticket_id,
    state_key,
    state_value,
    ttl_seconds
FROM `{self.full_dataset}.global_state_events`
WHERE event_timestamp >= TIMESTAMP('{query_start.isoformat()}')
  AND event_timestamp < TIMESTAMP('{query_end.isoformat()}')
ORDER BY event_timestamp ASC
""".strip()
    
    def _build_global_state_events_ticket_query(
        self,
        ticket_id: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> str:
        """
        Build query to get global_state_events for a specific ticket.
        
        With ticket_id now stored directly in the table, we can use
        direct equality filtering which is much faster with clustering.
        """
        time_filter = ""
        if start_time:
            time_filter += f"\n  AND event_timestamp >= TIMESTAMP('{start_time.isoformat()}')"
        if end_time:
            time_filter += f"\n  AND event_timestamp < TIMESTAMP('{end_time.isoformat()}')"
        
        return f"""
SELECT
    event_timestamp,
    ticket_id,
    state_key,
    state_value,
    ttl_seconds
FROM `{self.full_dataset}.global_state_events`
WHERE ticket_id = '{ticket_id}'{time_filter}
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
        
        With ticket_id stored directly in the table, this is now a simple
        direct filter - no joins or CTEs needed!
        """
        time_filter = ""
        if start_time:
            time_filter += f"\n  AND event_timestamp >= TIMESTAMP('{start_time.isoformat()}')"
        if end_time:
            time_filter += f"\n  AND event_timestamp < TIMESTAMP('{end_time.isoformat()}')"
        
        return f"""
SELECT *
FROM `{self.full_dataset}.{table}`
WHERE ticket_id = '{ticket_id}'{time_filter}
ORDER BY event_timestamp ASC
""".strip()