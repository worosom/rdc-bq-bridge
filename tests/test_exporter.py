"""Tests for BigQuery export functionality."""

import pytest
from datetime import datetime
from pathlib import Path

from src.exporter.export_config import (
    ExportConfig,
    TimeRangeExport,
    TicketExport
)
from src.exporter.query_builder import ExportQueryBuilder


class TestExportConfig:
    """Test export configuration models."""
    
    def test_time_range_export_validation(self):
        """Test time range export validation."""
        # Valid configuration
        export = TimeRangeExport(
            start_time=datetime(2025, 12, 1),
            end_time=datetime(2025, 12, 10),
            tables=["empatica", "blueiot"],
            format="parquet"
        )
        assert export.start_time < export.end_time
        
        # Invalid: start >= end
        with pytest.raises(ValueError):
            TimeRangeExport(
                start_time=datetime(2025, 12, 10),
                end_time=datetime(2025, 12, 1),
                tables=["empatica"],
                format="parquet"
            )
        
        # Invalid table name
        with pytest.raises(ValueError):
            TimeRangeExport(
                start_time=datetime(2025, 12, 1),
                end_time=datetime(2025, 12, 10),
                tables=["invalid_table"],
                format="parquet"
            )
    
    def test_ticket_export_validation(self):
        """Test ticket export validation."""
        # Valid configuration
        export = TicketExport(
            ticket_id="TICKET123",
            tables=["empatica", "blueiot"],
            format="avro"
        )
        assert export.ticket_id == "TICKET123"
        
        # Valid with time range
        export = TicketExport(
            ticket_id="TICKET123",
            start_time=datetime(2025, 12, 1),
            end_time=datetime(2025, 12, 10),
            tables=["empatica"],
            format="parquet"
        )
        assert export.start_time < export.end_time
    
    def test_export_config_defaults(self):
        """Test export configuration defaults."""
        config = ExportConfig()
        
        assert config.default_format == "parquet"
        assert config.parquet_compression == "snappy"
        assert config.avro_codec == "deflate"
        assert config.max_rows_per_export == 10_000_000


class TestQueryBuilder:
    """Test SQL query generation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.query_builder = ExportQueryBuilder(
            project_id="test-project",
            dataset_id="test_dataset"
        )
    
    def test_global_state_events_query(self):
        """Test query generation for global_state_events."""
        query = self.query_builder.build_time_range_query(
            table="global_state_events",
            start_time=datetime(2025, 12, 1, 0, 0, 0),
            end_time=datetime(2025, 12, 10, 23, 59, 59),
            include_ticket_id=False
        )
        
        assert "global_state_events" in query
        assert "event_timestamp" in query
        assert "state_key" in query
        assert "state_value" in query
        assert "2025-12-01" in query
        assert "2025-12-10" in query
    
    def test_biometric_query_without_ticket_id(self):
        """Test simple biometric query without ticket_id enrichment."""
        query = self.query_builder.build_time_range_query(
            table="empatica",
            start_time=datetime(2025, 12, 1),
            end_time=datetime(2025, 12, 10),
            include_ticket_id=False
        )
        
        assert "FROM `test-project.test_dataset.empatica`" in query
        assert "device_mappings" not in query  # No join
        assert "ORDER BY event_timestamp" in query
    
    def test_biometric_query_with_ticket_id(self):
        """Test biometric query with ticket_id enrichment."""
        query = self.query_builder.build_time_range_query(
            table="empatica",
            start_time=datetime(2025, 12, 1),
            end_time=datetime(2025, 12, 10),
            include_ticket_id=True
        )
        
        # Should include CTE for device mappings
        assert "WITH device_mappings AS" in query
        assert "EmpaticaDeviceID" in query
        assert "LEFT JOIN device_mappings" in query
        assert "ticket_id" in query
        assert "LEAD(" in query  # For device reassignment handling
    
    def test_ticket_query_for_global_state_events(self):
        """Test ticket query for global_state_events."""
        query = self.query_builder.build_ticket_query(
            table="global_state_events",
            ticket_id="TICKET123",
            start_time=datetime(2025, 12, 1),
            end_time=datetime(2025, 12, 10)
        )
        
        assert "Visitors:TICKET123:%" in query
        assert "event_timestamp >=" in query
        assert "2025-12-01" in query
    
    def test_ticket_query_for_biometric(self):
        """Test ticket query for biometric tables."""
        query = self.query_builder.build_ticket_query(
            table="empatica",
            ticket_id="TICKET123",
            start_time=datetime(2025, 12, 1),
            end_time=datetime(2025, 12, 10)
        )
        
        # Should resolve device_ids first
        assert "WITH ticket_devices AS" in query
        assert "Visitors:TICKET123:EmpaticaDeviceID" in query
        assert "INNER JOIN ticket_devices" in query
    
    def test_invalid_table_name(self):
        """Test that invalid table names raise error."""
        with pytest.raises(ValueError):
            self.query_builder.build_time_range_query(
                table="invalid_table",
                start_time=datetime(2025, 12, 1),
                end_time=datetime(2025, 12, 10)
            )


class TestFormatWriter:
    """Test format writers."""
    
    def test_parquet_import(self):
        """Test that pyarrow can be imported."""
        try:
            import pyarrow
            import pyarrow.parquet
            assert True
        except ImportError:
            pytest.skip("pyarrow not installed")
    
    def test_avro_import(self):
        """Test that fastavro can be imported."""
        try:
            import fastavro
            assert True
        except ImportError:
            pytest.skip("fastavro not installed")


# Integration tests would require actual BigQuery access
# These are marked to be skipped in CI
@pytest.mark.integration
class TestBigQueryExporterIntegration:
    """Integration tests requiring BigQuery access."""
    
    @pytest.mark.skip(reason="Requires BigQuery credentials")
    async def test_export_time_range(self):
        """Test actual time range export."""
        # This would require real credentials and data
        pass
    
    @pytest.mark.skip(reason="Requires BigQuery credentials")
    async def test_export_ticket(self):
        """Test actual ticket export."""
        # This would require real credentials and data
        pass
