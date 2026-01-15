"""Main BigQuery data export orchestrator."""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from google.auth import load_credentials_from_file
from google.cloud import bigquery

from ..config import Config
from .export_config import ExportConfig, TimeRangeExport, TicketExport
from .format_writer import FormatWriter
from .query_builder import ExportQueryBuilder

logger = logging.getLogger(__name__)


class BigQueryExporter:
    """Orchestrates data export from BigQuery to various formats."""
    
    def __init__(self, config: Config, export_config: Optional[ExportConfig] = None):
        """
        Initialize the exporter.
        
        Args:
            config: Main application configuration
            export_config: Export-specific configuration (uses defaults if not provided)
        """
        self.config = config
        self.export_config = export_config or ExportConfig()
        self.bq_client: Optional[bigquery.Client] = None
        self.query_builder = ExportQueryBuilder(
            config.gcp.project_id,
            config.gcp.dataset_id
        )
        
        # Initialize FormatWriter with table configs if available
        table_configs = {}
        if config.bigquery and config.bigquery.tables:
            table_configs = config.bigquery.tables
        self.format_writer = FormatWriter(table_configs)
    
    async def initialize(self) -> None:
        """Initialize BigQuery client."""
        try:
            # Load credentials from service account file
            credentials, _ = load_credentials_from_file(
                self.config.gcp.credentials_file,
                scopes=[
                    'https://www.googleapis.com/auth/bigquery',
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            )
            
            # Initialize BigQuery client
            self.bq_client = bigquery.Client(
                project=self.config.gcp.project_id,
                credentials=credentials
            )
            
            logger.info("BigQuery exporter initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery exporter: {e}")
            raise
    
    async def export_time_range(
        self,
        export_spec: TimeRangeExport
    ) -> dict[str, Path]:
        """
        Export all data within a time range.
        
        Args:
            export_spec: Time range export specification
        
        Returns:
            Dictionary mapping table names to output file paths
        """
        if not self.bq_client:
            await self.initialize()
        
        logger.info(
            f"Starting time range export: {export_spec.start_time} to {export_spec.end_time}, "
            f"tables: {export_spec.tables}, format: {export_spec.format}"
        )
        
        # Determine output directory
        if export_spec.output_path:
            output_dir = export_spec.output_path
            if not output_dir.is_dir():
                output_dir = output_dir.parent
        else:
            # Create directory named by time range
            start_str = export_spec.start_time.strftime("%Y%m%d_%H%M%S")
            end_str = export_spec.end_time.strftime("%Y%m%d_%H%M%S")
            output_dir = self.export_config.output_dir / f"timerange_{start_str}_to_{end_str}"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
        
        # For AVRO format, collect all DataFrames first to combine them
        if export_spec.format == "avro":
            return await self._export_time_range_combined_avro(
                export_spec,
                output_dir
            )
        
        # Export each table separately for other formats
        output_files = {}
        
        for table in export_spec.tables:
            try:
                logger.info(f"Exporting table: {table}")
                
                # Generate query
                query = self.query_builder.build_time_range_query(
                    table,
                    export_spec.start_time,
                    export_spec.end_time,
                    include_ticket_id=export_spec.include_ticket_id
                )
                
                logger.debug(f"Query for {table}:\n{query}")
                
                # Execute query and get DataFrame
                df = await self._execute_query_to_dataframe(query)
                
                if len(df) == 0:
                    logger.warning(f"No data found for table {table} in time range")
                    continue
                
                logger.info(f"Retrieved {len(df)} rows from {table}")
                
                # Write to file
                output_path = await self._write_dataframe(
                    df,
                    table,
                    output_dir,
                    export_spec.format
                )
                
                output_files[table] = output_path
                
            except Exception as e:
                logger.error(f"Failed to export table {table}: {e}")
                raise
        
        logger.info(f"Time range export completed. Files: {list(output_files.values())}")
        return output_files
    
    async def export_by_ticket(
        self,
        export_spec: TicketExport
    ) -> dict[str, Path]:
        """
        Export all data for a specific ticket_id.
        
        Args:
            export_spec: Ticket export specification
        
        Returns:
            Dictionary mapping table names to output file paths
        """
        if not self.bq_client:
            await self.initialize()
        
        logger.info(
            f"Starting ticket export: ticket_id={export_spec.ticket_id}, "
            f"tables: {export_spec.tables}, format: {export_spec.format}"
        )
        
        # Determine output directory
        if export_spec.output_path:
            output_dir = export_spec.output_path
            if not output_dir.is_dir():
                output_dir = output_dir.parent
        else:
            output_dir = self.export_config.output_dir / f"ticket_{export_spec.ticket_id}"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
        
        # For AVRO format, collect all DataFrames first to combine them
        if export_spec.format == "avro":
            output_files = await self._export_ticket_combined_avro(
                export_spec,
                output_dir
            )
        else:
            # Export each table separately for other formats
            output_files = {}
            
            for table in export_spec.tables:
                try:
                    logger.info(f"Exporting table: {table}")
                    
                    # Generate query
                    query = self.query_builder.build_ticket_query(
                        table,
                        export_spec.ticket_id,
                        export_spec.start_time,
                        export_spec.end_time
                    )
                    
                    logger.debug(f"Query for {table}:\n{query}")
                    
                    # Execute query and get DataFrame
                    df = await self._execute_query_to_dataframe(query)
                    
                    if len(df) == 0:
                        logger.warning(f"No data found for table {table} and ticket {export_spec.ticket_id}")
                        continue
                    
                    logger.info(f"Retrieved {len(df)} rows from {table}")
                    
                    # Write to file
                    output_path = await self._write_dataframe(
                        df,
                        table,
                        output_dir,
                        export_spec.format
                    )
                    
                    output_files[table] = output_path
                    
                except Exception as e:
                    logger.error(f"Failed to export table {table}: {e}")
                    raise
        
        # Write metadata file
        metadata_path = output_dir / "metadata.json"
        await self._write_metadata(export_spec, output_files, metadata_path)
        
        logger.info(f"Ticket export completed. Files: {list(output_files.values())}")
        return output_files
    
    async def _execute_query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute BigQuery query and return results as DataFrame.
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame with query results
        """
        try:
            # Execute query
            query_job = self.bq_client.query(query)
            
            # Wait for completion and convert to DataFrame
            df = query_job.to_dataframe()
            
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    async def _write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        output_dir: Path,
        format: str
    ) -> Path:
        """
        Write DataFrame to file in specified format.
        
        Args:
            df: DataFrame to write
            table_name: Name of the table (used in filename)
            output_dir: Output directory
            format: File format (parquet, avro, jsonl)
        
        Returns:
            Path to written file
        """
        if format == "parquet":
            output_path = output_dir / f"{table_name}.parquet"
            self.format_writer.write_parquet(
                df,
                output_path,
                compression=self.export_config.parquet_compression,
                row_group_size=self.export_config.parquet_row_group_size
            )
        
        elif format == "avro":
            output_path = output_dir / f"{table_name}.avro"
            self.format_writer.write_avro_for_replay(
                df,
                output_path,
                codec=self.export_config.avro_codec
            )
        
        elif format == "jsonl":
            output_path = output_dir / f"{table_name}.jsonl"
            self.format_writer.write_jsonl(df, output_path)
        
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        return output_path
    
    async def _write_metadata(
        self,
        export_spec: TicketExport,
        output_files: dict[str, Path],
        metadata_path: Path
    ) -> None:
        """Write export metadata to JSON file."""
        import json
        from datetime import datetime
        
        metadata = {
            "ticket_id": export_spec.ticket_id,
            "export_timestamp": datetime.utcnow().isoformat(),
            "tables": export_spec.tables,
            "format": export_spec.format,
            "time_range": {
                "start": export_spec.start_time.isoformat() if export_spec.start_time else None,
                "end": export_spec.end_time.isoformat() if export_spec.end_time else None
            },
            "files": {
                table: str(path)
                for table, path in output_files.items()
            }
        }
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Wrote metadata to {metadata_path}")
    
    async def _export_time_range_combined_avro(
        self,
        export_spec: TimeRangeExport,
        output_dir: Path
    ) -> dict[str, Path]:
        """
        Export time range data to a single combined AVRO file ordered by timestamp.
        
        Args:
            export_spec: Time range export specification
            output_dir: Output directory
        
        Returns:
            Dictionary with single entry mapping "combined" to output file path
        """
        logger.info("Exporting to combined AVRO file ordered by timestamp")
        
        # Collect DataFrames from all tables
        dataframes = {}
        
        for table in export_spec.tables:
            try:
                logger.info(f"Querying table: {table}")
                
                # Generate query
                query = self.query_builder.build_time_range_query(
                    table,
                    export_spec.start_time,
                    export_spec.end_time,
                    include_ticket_id=export_spec.include_ticket_id
                )
                
                logger.debug(f"Query for {table}:\n{query}")
                
                # Execute query and get DataFrame
                df = await self._execute_query_to_dataframe(query)
                
                if len(df) == 0:
                    logger.warning(f"No data found for table {table} in time range")
                    continue
                
                logger.info(f"Retrieved {len(df)} rows from {table}")
                dataframes[table] = df
                
            except Exception as e:
                logger.error(f"Failed to query table {table}: {e}")
                raise
        
        if not dataframes:
            logger.warning("No data found in any table")
            return {}
        
        # Write combined AVRO file
        output_path = output_dir / "combined.avro"
        self.format_writer.write_combined_avro_for_replay(
            dataframes,
            output_path,
            codec=self.export_config.avro_codec
        )
        
        return {"combined": output_path}
    
    async def _export_ticket_combined_avro(
        self,
        export_spec: TicketExport,
        output_dir: Path
    ) -> dict[str, Path]:
        """
        Export ticket data to a single combined AVRO file ordered by timestamp.
        
        Args:
            export_spec: Ticket export specification
            output_dir: Output directory
        
        Returns:
            Dictionary with single entry mapping "combined" to output file path
        """
        logger.info("Exporting to combined AVRO file ordered by timestamp")
        
        # Collect DataFrames from all tables
        dataframes = {}
        
        for table in export_spec.tables:
            try:
                logger.info(f"Querying table: {table}")
                
                # Generate query
                query = self.query_builder.build_ticket_query(
                    table,
                    export_spec.ticket_id,
                    export_spec.start_time,
                    export_spec.end_time
                )
                
                logger.debug(f"Query for {table}:\n{query}")
                
                # Execute query and get DataFrame
                df = await self._execute_query_to_dataframe(query)
                
                if len(df) == 0:
                    logger.warning(f"No data found for table {table} and ticket {export_spec.ticket_id}")
                    continue
                
                logger.info(f"Retrieved {len(df)} rows from {table}")
                dataframes[table] = df
                
            except Exception as e:
                logger.error(f"Failed to query table {table}: {e}")
                raise
        
        if not dataframes:
            logger.warning("No data found in any table")
            return {}
        
        # Write combined AVRO file
        output_path = output_dir / f"{export_spec.ticket_id}_combined.avro"
        self.format_writer.write_combined_avro_for_replay(
            dataframes,
            output_path,
            codec=self.export_config.avro_codec
        )
        
        return {"combined": output_path}
    
    async def list_available_tickets(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> list[dict]:
        """
        List all available ticket_ids in the dataset.
        
        Args:
            start_time: Optional start time filter (ISO format)
            end_time: Optional end time filter (ISO format)
        
        Returns:
            List of dictionaries with ticket info
        """
        if not self.bq_client:
            await self.initialize()
        
        time_filter = ""
        if start_time:
            time_filter += f"\n  AND event_timestamp >= TIMESTAMP('{start_time}')"
        if end_time:
            time_filter += f"\n  AND event_timestamp < TIMESTAMP('{end_time}')"
        
        query = f"""
SELECT DISTINCT
  REGEXP_EXTRACT(state_key, r'Visitors:([^:]+):') as ticket_id,
  MIN(event_timestamp) as first_seen,
  MAX(event_timestamp) as last_seen,
  COUNT(*) as event_count
FROM `{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.global_state_events`
WHERE state_key LIKE 'Visitors:%:%'{time_filter}
GROUP BY ticket_id
HAVING ticket_id IS NOT NULL
ORDER BY first_seen DESC
"""
        
        df = await self._execute_query_to_dataframe(query)
        
        return df.to_dict('records')