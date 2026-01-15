"""BigQuery dataset and table setup utilities for the RDC-BigQuery bridge."""

import asyncio
import logging
from typing import List, Optional

from google.auth import load_credentials_from_file
from google.cloud import bigquery
from google.cloud.bigquery_storage_v1.services.big_query_write import BigQueryWriteAsyncClient
from google.cloud.bigquery_storage_v1.types import CreateWriteStreamRequest, WriteStream
from google.cloud.exceptions import Conflict, NotFound

from .config import Config
from .bq_schema_utils import (
    get_table_schema,
    get_table_partitioning,
    get_table_clustering_fields,
    get_table_description,
)

logger = logging.getLogger(__name__)


class BigQuerySetup:
    """Handles BigQuery dataset and table creation with proper schemas."""

    def __init__(self, config: Config):
        self.config = config
        self.client: Optional[bigquery.Client] = None

    async def initialize(self) -> None:
        """Initialize the BigQuery client."""
        try:
            # Load credentials from service account file
            credentials, project = load_credentials_from_file(
                self.config.gcp.credentials_file,
                scopes=['https://www.googleapis.com/auth/bigquery',
                        'https://www.googleapis.com/auth/cloud-platform']
            )

            # Initialize BigQuery client
            self.client = bigquery.Client(
                project=self.config.gcp.project_id,
                credentials=credentials
            )

            logger.info("BigQuery setup client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize BigQuery setup client: {e}")
            raise

    async def setup_dataset_and_tables(self) -> None:
        """Create dataset and all required tables."""
        if not self.client:
            await self.initialize()

        # Create dataset
        await self.create_dataset()

        # Track if any tables were newly created
        tables_created = []
        
        # Create tables and track which ones were new
        if await self.create_table("empatica"):
            tables_created.append("empatica")
        if await self.create_table("blueiot"):
            tables_created.append("blueiot")
        if await self.create_table("global_state_events"):
            tables_created.append("global_state_events")

        # If any tables were newly created, wait for them to be fully ready
        if tables_created:
            logger.info(f"Newly created tables: {tables_created}. Waiting for tables to be fully available...")
            # Wait 5 seconds for table propagation
            await asyncio.sleep(5)
            
            # Verify tables are accessible with retry
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    all_ready = True
                    for table_name in tables_created:
                        table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.{table_name}"
                        try:
                            table = self.client.get_table(table_id)
                            logger.debug(f"Table {table_name} is accessible (attempt {attempt + 1})")
                        except Exception as e:
                            logger.warning(f"Table {table_name} not yet accessible: {e}")
                            all_ready = False
                            break
                    
                    if all_ready:
                        logger.info("All newly created tables are now accessible")
                        break
                    else:
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4, 8 seconds
                            logger.info(f"Waiting {wait_time} seconds before retry...")
                            await asyncio.sleep(wait_time)
                except Exception as e:
                    logger.warning(f"Error verifying table accessibility (attempt {attempt + 1}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)

        logger.info("BigQuery dataset and tables setup completed successfully")

    async def create_dataset(self) -> None:
        """Create the BigQuery dataset if it doesn't exist."""
        dataset_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}"

        try:
            # Check if dataset already exists
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
            return

        except NotFound:
            # Dataset doesn't exist, create it
            pass

        try:
            # Create dataset
            dataset = bigquery.Dataset(dataset_id)
            dataset.description = "Real-time telemetry data from RDC installation"
            dataset.location = "us-west1"  # You can make this configurable

            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset_id}")

        except Conflict:
            logger.info(
                f"Dataset {dataset_id} already exists (created concurrently)")
        except Exception as e:
            logger.error(f"Failed to create dataset {dataset_id}: {e}")
            raise

    async def create_table(self, table_name: str) -> bool:
        """Create a table with the appropriate schema, partitioning, and clustering.
        
        Returns:
            bool: True if table was newly created, False if it already existed
        """
        table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.{table_name}"

        try:
            # Check if table already exists
            self.client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
            return False

        except NotFound:
            # Table doesn't exist, create it
            pass

        try:
            # Get schema and configuration from centralized definitions
            schema = get_table_schema(table_name)

            # Create table
            table = bigquery.Table(table_id, schema=schema)

            # Set description
            description = get_table_description(table_name)
            if description:
                table.description = description

            # Set up partitioning
            partitioning = get_table_partitioning(table_name)
            if partitioning:
                table.time_partitioning = partitioning

            # Set up clustering
            clustering_fields = get_table_clustering_fields(table_name)
            if clustering_fields:
                table.clustering_fields = clustering_fields

            # Create the table
            table = self.client.create_table(table, timeout=30)
            logger.info(
                f"Created table {table_name} with partitioning and clustering")
            return True

        except Conflict:
            logger.info(
                f"Table {table_id} already exists (created concurrently)")
            return False
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise

    async def verify_tables_exist(self) -> bool:
        """Verify that all required tables exist and are properly configured."""
        if not self.client:
            await self.initialize()

        required_tables = ["empatica", "blueiot", "global_state_events"]

        try:
            for table_name in required_tables:
                table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.{table_name}"
                table = self.client.get_table(table_id)

                # Verify partitioning
                if not table.time_partitioning:
                    logger.warning(
                        f"Table {table_name} is missing time partitioning")
                    return False

                # Verify clustering
                if not table.clustering_fields:
                    logger.warning(f"Table {table_name} is missing clustering")
                    return False

                logger.info(
                    f"Table {table_name} exists and is properly configured")

            return True

        except NotFound as e:
            logger.error(f"Required table not found: {e}")
            return False
        except Exception as e:
            logger.error(f"Error verifying tables: {e}")
            return False

    async def verify_storage_write_api_ready(self) -> bool:
        """Verify that tables are ready for Storage Write API by attempting to create test write streams."""
        if not self.client:
            await self.initialize()

        required_tables = ["empatica", "blueiot", "global_state_events"]
        
        # Load credentials
        if self.config.gcp.credentials_file is not None:
            credentials, _ = load_credentials_from_file(
                self.config.gcp.credentials_file,
                scopes=['https://www.googleapis.com/auth/bigquery',
                        'https://www.googleapis.com/auth/cloud-platform']
            )
        else:
            credentials = None

        # Create async write client
        async_write_client = BigQueryWriteAsyncClient(credentials=credentials)
        
        try:
            for table_name in required_tables:
                table_resource_path = f"projects/{self.config.gcp.project_id}/datasets/{self.config.gcp.dataset_id}/tables/{table_name}"
                
                try:
                    # Try to create a test write stream
                    request = CreateWriteStreamRequest(
                        parent=table_resource_path,
                        write_stream=WriteStream(type_=WriteStream.Type.COMMITTED)
                    )
                    
                    test_stream = await async_write_client.create_write_stream(request)
                    logger.debug(f"Successfully created test write stream for {table_name}: {test_stream.name}")
                    
                    # Clean up the test stream by finalizing it
                    try:
                        from google.cloud.bigquery_storage_v1.types import FinalizeWriteStreamRequest
                        finalize_request = FinalizeWriteStreamRequest(name=test_stream.name)
                        await async_write_client.finalize_write_stream(finalize_request)
                        logger.debug(f"Finalized test write stream for {table_name}")
                    except Exception as finalize_error:
                        logger.debug(f"Error finalizing test stream (non-critical): {finalize_error}")
                    
                except Exception as e:
                    logger.warning(f"Storage Write API not ready for table {table_name}: {e}")
                    await async_write_client.transport.close()
                    return False
            
            await async_write_client.transport.close()
            logger.info("Storage Write API is ready for all tables")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying Storage Write API readiness: {e}")
            try:
                await async_write_client.transport.close()
            except:
                pass
            return False

    async def get_table_info(self, table_name: str) -> dict:
        """Get information about a specific table."""
        if not self.client:
            await self.initialize()

        table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.{table_name}"

        try:
            table = self.client.get_table(table_id)

            return {
                "table_id": table.table_id,
                "created": table.created,
                "modified": table.modified,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "schema_fields": len(table.schema),
                "partitioning": {
                    "type": table.time_partitioning.type_ if table.time_partitioning else None,
                    "field": table.time_partitioning.field if table.time_partitioning else None,
                } if table.time_partitioning else None,
                "clustering_fields": table.clustering_fields,
                "description": table.description,
            }

        except NotFound:
            return {"error": f"Table {table_name} not found"}
        except Exception as e:
            return {"error": f"Error getting table info: {e}"}


async def setup_bigquery_infrastructure(config: Config) -> None:
    """Convenience function to set up all BigQuery infrastructure."""
    setup = BigQuerySetup(config)
    await setup.setup_dataset_and_tables()


async def verify_bigquery_infrastructure(config: Config) -> bool:
    """Convenience function to verify BigQuery infrastructure exists."""
    setup = BigQuerySetup(config)
    return await setup.verify_tables_exist()
