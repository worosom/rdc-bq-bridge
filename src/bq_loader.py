"""BigQuery loader with protobuf handling for the RDC-BigQuery bridge."""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.auth import load_credentials_from_file
from google.cloud import bigquery
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient
from google.cloud.bigquery_storage_v1.services.big_query_write import BigQueryWriteAsyncClient
from google.cloud.bigquery_storage_v1.types import (
    AppendRowsRequest,
    CreateWriteStreamRequest,
    ProtoRows,
    ProtoSchema,
    WriteStream
)
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import Descriptor
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .bq_schema_utils import create_proto_descriptor, create_proto_message
from .config import Config
from .row_assembler import AssembledRow

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Handles loading data into BigQuery using the Storage Write API."""

    def __init__(self, config: Config, table_name: str, input_queue: asyncio.Queue[AssembledRow], dry_run: bool = False):
        self.config = config
        self.table_name = table_name
        self.input_queue = input_queue
        self.dry_run = dry_run

        # BigQuery clients
        self.bq_client: Optional[bigquery.Client] = None
        self.write_client: Optional[BigQueryWriteClient] = None
        self.async_write_client: Optional[BigQueryWriteAsyncClient] = None

        # Write stream
        self.write_stream: Optional[WriteStream] = None
        self.proto_descriptor: Optional[Descriptor] = None
        self.descriptor_proto: Optional[descriptor_pb2.DescriptorProto] = None

        # Batching configuration
        self.batch_size_rows = config.loader.batch_size_rows
        self.batch_size_bytes = config.loader.batch_size_bytes
        self.commit_interval = config.loader.commit_interval_seconds

        # Current batch
        self.current_batch: List[AssembledRow] = []
        self.current_batch_size = 0
        self.last_commit_time = time.time()

        # Statistics
        self.rows_processed = 0
        self.batches_committed = 0
        self.errors_count = 0

        self._running = False

        if dry_run:
            logger.info(f"Initialized BigQueryLoader for table {table_name} in DRY-RUN mode")
        else:
            logger.info(f"Initialized BigQueryLoader for table {table_name}")

    async def start(self) -> None:
        """Start the BigQuery loader."""
        logger.info(f"Starting BigQuery loader for table {self.table_name}...")

        try:
            # Initialize clients and write stream (skip in dry-run mode)
            if not self.dry_run:
                await self._initialize_clients()
                await self._create_write_stream()

            self._running = True

            # Start the main processing loop
            await self._process_rows()

        except Exception as e:
            logger.error(
                f"Error in BigQuery loader for {self.table_name}: {e}")
            logger.exception(e)
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the BigQuery loader."""
        logger.info(f"Stopping BigQuery loader for table {self.table_name}...")
        self._running = False

        # Commit any remaining batch
        if self.current_batch:
            await self._commit_batch()

        # Clean up write stream
        if self.write_stream and self.write_client:
            try:
                # Finalize the write stream
                await self._finalize_write_stream()
            except Exception as e:
                logger.error(f"Error finalizing write stream: {e}")

        # Close clients
        if self.async_write_client:
            await self.async_write_client.transport.close()

        logger.info(
            f"BigQuery loader stopped. Processed {self.rows_processed} rows in {self.batches_committed} batches")

    async def _initialize_clients(self) -> None:
        """Initialize BigQuery clients."""
        try:
            if self.config.gcp.credentials_file is not None:
                # Load credentials from service account file
                credentials, project = load_credentials_from_file(
                    self.config.gcp.credentials_file,
                    scopes=['https://www.googleapis.com/auth/bigquery',
                            'https://www.googleapis.com/auth/cloud-platform']
                )
            else:
                credentials = None

            # Initialize BigQuery client for metadata operations
            self.bq_client = bigquery.Client(
                project=self.config.gcp.project_id,
                credentials=credentials
            )

            # Initialize BigQuery Write client for data ingestion
            self.write_client = BigQueryWriteClient(
                credentials=credentials
            )

            # Initialize async write client
            self.async_write_client = BigQueryWriteAsyncClient(
                credentials=credentials
            )

            logger.info("BigQuery clients initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize BigQuery clients: {e}")
            raise

    async def _create_write_stream(self) -> None:
        """Create a write stream for the table with retry logic."""
        max_retries = 5
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Get table reference in standard SQL format for BigQuery client
                table_id = f"{self.config.gcp.project_id}.{self.config.gcp.dataset_id}.{self.table_name}"

                # Get table reference in resource path format for Storage Write API
                table_resource_path = f"projects/{self.config.gcp.project_id}/datasets/{self.config.gcp.dataset_id}/tables/{self.table_name}"

                # Get table schema from BigQuery using standard SQL format
                table = self.bq_client.get_table(table_id)

                # Create protobuf descriptor from BigQuery schema
                self.proto_descriptor, self.descriptor_proto = create_proto_descriptor(
                    table.schema, self.table_name)

                # Create write stream request using resource path format
                # Note: proto_schema is not set on WriteStream creation, it's provided per append request
                request = CreateWriteStreamRequest(
                    parent=table_resource_path,
                    write_stream=WriteStream(
                        type_=WriteStream.Type.COMMITTED
                    )
                )

                # Create the write stream
                self.write_stream = await self.async_write_client.create_write_stream(request)

                logger.info(f"Created write stream: {self.write_stream.name}")
                return  # Success - exit the retry loop

            except Exception as e:
                error_msg = str(e)
                is_not_found = "404" in error_msg or "NOT_FOUND" in error_msg or "not found" in error_msg.lower()
                
                if is_not_found and attempt < max_retries - 1:
                    # Table not ready for Storage Write API yet, retry with exponential backoff
                    wait_time = base_delay ** (attempt + 1)  # 2, 4, 8, 16, 32 seconds
                    logger.warning(
                        f"Table {self.table_name} not ready for write stream creation (attempt {attempt + 1}/{max_retries}). "
                        f"Waiting {wait_time} seconds before retry... Error: {error_msg}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    # Either not a 404 error, or we've exhausted retries
                    logger.error(f"Failed to create write stream after {attempt + 1} attempts: {e}")
                    raise

    async def _process_rows(self) -> None:
        """Main processing loop for incoming rows."""
        logger.info(f"Starting row processing for table {self.table_name}")
        rows_received = 0

        while self._running:
            try:
                # Wait for rows with a timeout to check commit interval
                try:
                    row = await asyncio.wait_for(self.input_queue.get(), timeout=1.0)
                    rows_received += 1

                    # Log every row received for debugging
                    logger.debug(
                        f"[LOADER] Table {self.table_name} received row #{rows_received}, target_table: {row.target_table}")

                    # Verify this row is for our table (should always be true with dedicated queues)
                    if row.target_table != self.table_name:
                        logger.error(
                            f"Loader for {self.table_name} received row for wrong table: {row.target_table}")
                        continue

                    await self._add_to_batch(row)
                    logger.debug(f"[LOADER] Row added to batch (batch_size={len(self.current_batch)})")

                except asyncio.TimeoutError:
                    # Check if we should commit based on time interval
                    if self._should_commit_by_time():
                        logger.info(f"[LOADER] Committing batch due to time interval (batch_size={len(self.current_batch)})")
                        await self._commit_batch()
                    continue

                # Check if we should commit the batch
                if self._should_commit_batch():
                    logger.info(f"[LOADER] Committing batch due to size threshold (batch_size={len(self.current_batch)})")
                    await self._commit_batch()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Error processing row in {self.table_name} loader: {e}", exc_info=True)
                self.errors_count += 1
                continue

        logger.info(
            f"Stopped row processing for table {self.table_name}, total rows received: {rows_received}")

    async def _add_to_batch(self, row: AssembledRow) -> None:
        """Add a row to the current batch."""
        self.current_batch.append(row)

        # Estimate row size (rough approximation)
        row_size = len(str(row.data).encode('utf-8'))
        self.current_batch_size += row_size

        logger.debug(
            f"Added row to batch. Batch size: {len(self.current_batch)} rows, ~{self.current_batch_size} bytes")

    def _should_commit_batch(self) -> bool:
        """Check if the current batch should be committed."""
        return (
            len(self.current_batch) >= self.batch_size_rows or
            self.current_batch_size >= self.batch_size_bytes or
            self._should_commit_by_time()
        )

    def _should_commit_by_time(self) -> bool:
        """Check if the batch should be committed based on time interval."""
        return (
            len(self.current_batch) > 0 and
            time.time() - self.last_commit_time >= self.commit_interval
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((Exception,))
    )
    async def _commit_batch(self) -> None:
        """Commit the current batch to BigQuery."""
        if not self.current_batch:
            return

        batch_size = len(self.current_batch)
        
        # In dry-run mode, just log and skip the actual commit
        if self.dry_run:
            logger.info(
                f"[DRY-RUN] Would commit batch of {batch_size} rows to {self.table_name}")
            # Update statistics as if we committed
            self.rows_processed += batch_size
            self.batches_committed += 1
            self.last_commit_time = time.time()
            # Clear the batch
            self.current_batch.clear()
            self.current_batch_size = 0
            return
        
        logger.info(
            f"Committing batch of {batch_size} rows to {self.table_name}")

        try:
            logger.debug("Step 1: Converting rows to protobuf messages")
            # Convert rows to protobuf messages
            proto_rows = await self._create_proto_rows(self.current_batch)
            logger.debug(
                f"Step 1: Successfully created proto_rows with {len(proto_rows.serialized_rows)} rows")

            logger.debug("Step 2: Creating proto schema for the request")
            # Create proto schema using the DescriptorProto object
            logger.debug(
                f"descriptor_proto type: {type(self.descriptor_proto)}")

            # ProtoSchema needs to be initialized with a dictionary
            # Initialize with an empty dict first, then set the proto_descriptor
            proto_schema = ProtoSchema({})

            # The proto_descriptor field in ProtoSchema is of type DescriptorProto
            # We need to copy our descriptor_proto to it
            proto_schema.proto_descriptor.CopyFrom(self.descriptor_proto)

            logger.debug(
                "Step 2: Successfully created ProtoSchema with DescriptorProto object")

            logger.debug("Step 3: Creating AppendRowsRequest.ProtoData")
            # Create append request with proper structure
            try:
                proto_data = AppendRowsRequest.ProtoData(
                    writer_schema=proto_schema,
                    rows=proto_rows
                )
                logger.debug(
                    "Step 3: Successfully created AppendRowsRequest.ProtoData")
            except Exception as e:
                logger.error(
                    f"Step 3: Failed to create AppendRowsRequest.ProtoData: {e}")
                logger.error(f"proto_schema type: {type(proto_schema)}")
                logger.error(f"proto_rows type: {type(proto_rows)}")
                raise

            logger.debug("Step 4: Creating AppendRowsRequest")
            request = AppendRowsRequest(
                write_stream=self.write_stream.name,
                proto_rows=proto_data
            )
            logger.debug("Step 4: Successfully created AppendRowsRequest")

            # Create an async generator for the request
            async def request_generator():
                yield request

            # Append rows using the async client with retry on stream expiration
            max_stream_retries = 2
            for stream_retry in range(max_stream_retries):
                try:
                    response_stream = await self.async_write_client.append_rows(request_generator())
                    
                    # Process response
                    async for append_result in response_stream:
                        # Check if there's an actual error by looking for error code or message
                        # The error field might be present but empty, which shouldn't be treated as an error
                        has_error = False
                        if hasattr(append_result, 'error') and append_result.error:
                            # Check if the error has meaningful content
                            error_code = getattr(append_result.error, 'code', None)
                            error_message = getattr(
                                append_result.error, 'message', None)

                            # Only treat it as an error if there's an actual error code or message
                            # Error code 0 with no message is not a real error
                            if (error_code and error_code != 0) or error_message:
                                has_error = True

                        if has_error:
                            logger.error(f"BigQuery append failed with error:")
                            logger.error(
                                f"  Error code: {append_result.error.code if hasattr(append_result.error, 'code') else 'N/A'}")
                            logger.error(
                                f"  Error message: {append_result.error.message if hasattr(append_result.error, 'message') else 'N/A'}")
                            logger.error(
                                f"  Error details: {append_result.error.details if hasattr(append_result.error, 'details') else 'N/A'}")
                            logger.error(f"  Full error object: {append_result.error}")
                            logger.error(f"  Full append_result: {append_result}")

                            # Try to get row errors if available
                            if hasattr(append_result, 'row_errors') and append_result.row_errors:
                                logger.error(
                                    f"Row errors ({len(append_result.row_errors)}):")
                                for i, row_error in enumerate(append_result.row_errors):
                                    logger.error(f"  Row {i}: {row_error}")

                            self.errors_count += 1
                            raise Exception(
                                f"BigQuery append error: {append_result.error}")
                        else:
                            # Success case - check if we have an offset which indicates successful append
                            if hasattr(append_result, 'offset'):
                                logger.debug(
                                    f"Successfully appended {batch_size} rows")
                                logger.debug(
                                    f"Append result offset: {append_result.offset}")
                            else:
                                logger.debug(f"Append completed for {batch_size} rows")
                                logger.debug(f"Append result: {append_result}")
                            break  # We only expect one response for our single request
                    
                    # If we made it here, the append succeeded - break out of retry loop
                    break
                    
                except Exception as e:
                    error_str = str(e)
                    # Check if this is a NOT_FOUND error (stream expired or invalid)
                    is_stream_not_found = "NOT_FOUND" in error_str or "404" in error_str or "Stream is not found" in error_str
                    
                    if is_stream_not_found and stream_retry < max_stream_retries - 1:
                        logger.warning(
                            f"Write stream not found (attempt {stream_retry + 1}/{max_stream_retries}), recreating stream: {e}")
                        await self._recreate_write_stream()
                        
                        # Update request with new stream name
                        request = AppendRowsRequest(
                            write_stream=self.write_stream.name,
                            proto_rows=proto_data
                        )
                        
                        # Recreate the request generator for the retry
                        async def request_generator():
                            yield request
                        
                        # Retry will happen in the next loop iteration
                        continue
                    else:
                        # Either not a stream error, or we've exhausted retries
                        if is_stream_not_found:
                            logger.error(
                                f"Failed to append after {stream_retry + 1} stream recreation attempts")
                        raise

            # Update statistics
            self.rows_processed += batch_size
            self.batches_committed += 1
            self.last_commit_time = time.time()

            # Clear the batch
            self.current_batch.clear()
            self.current_batch_size = 0

            logger.info(f"Successfully committed batch to {self.table_name}")

        except Exception as e:
            logger.error(f"Failed to commit batch: {e}")
            logger.exception(e)
            self.errors_count += 1
            raise

    async def _create_proto_rows(self, rows: List[AssembledRow]) -> ProtoRows:
        """Convert assembled rows to protobuf format."""
        proto_rows = ProtoRows()

        for i, row in enumerate(rows):
            try:
                # Create protobuf message from row data
                proto_message = create_proto_message(
                    self.proto_descriptor,
                    row.data,
                    self.table_name
                )

                # Serialize the message
                serialized_row = proto_message.SerializeToString()
                proto_rows.serialized_rows.append(serialized_row)

            except Exception as e:
                logger.error(f"Error creating proto message for row {i+1}/{len(rows)}: {e}")
                logger.error(f"Row data: {row.data}")
                logger.error(f"Row ID: {row.row_id}")
                logger.error(f"Target table: {row.target_table}")
                raise

        return proto_rows

    async def _recreate_write_stream(self) -> None:
        """Recreate the write stream when it expires or becomes invalid."""
        logger.info(f"Recreating write stream for table {self.table_name}")

        try:
            # Try to finalize the old stream (ignore errors)
            if self.write_stream:
                try:
                    from google.cloud.bigquery_storage_v1.types import FinalizeWriteStreamRequest
                    request = FinalizeWriteStreamRequest(
                        name=self.write_stream.name)
                    await self.async_write_client.finalize_write_stream(request)
                except Exception as e:
                    logger.debug(
                        f"Error finalizing old stream (expected): {e}")

            # Create a new write stream
            await self._create_write_stream()
            logger.info(
                f"Successfully recreated write stream: {self.write_stream.name}")

        except Exception as e:
            logger.error(f"Failed to recreate write stream: {e}")
            raise

    async def _finalize_write_stream(self) -> None:
        """Finalize the write stream."""
        if not self.write_stream:
            return

        try:
            # Finalize the write stream to make data available for querying
            from google.cloud.bigquery_storage_v1.types import FinalizeWriteStreamRequest

            request = FinalizeWriteStreamRequest(
                name=self.write_stream.name
            )

            await self.async_write_client.finalize_write_stream(request)
            logger.info(f"Finalized write stream: {self.write_stream.name}")

        except Exception as e:
            logger.error(f"Error finalizing write stream: {e}")
            raise

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics about the loader."""
        return {
            "table_name": self.table_name,
            "rows_processed": self.rows_processed,
            "batches_committed": self.batches_committed,
            "errors_count": self.errors_count,
            "current_batch_size": len(self.current_batch),
            "current_batch_bytes": self.current_batch_size,
            "running": self._running
        }


class BigQueryLoaderManager:
    """Manages multiple BigQuery loaders for different tables."""

    def __init__(self, config: Config, input_queue: asyncio.Queue[AssembledRow], dry_run: bool = False):
        self.config = config
        self.input_queue = input_queue
        self.dry_run = dry_run
        self.loaders: Dict[str, BigQueryLoader] = {}
        self.table_queues: Dict[str, asyncio.Queue[AssembledRow]] = {}
        self._running = False

        # Get unique target tables from routing rules
        target_tables = set(
            rule.target_table for rule in config.pipeline.routing_rules)

        # Create a separate queue and loader for each target table
        for table_name in target_tables:
            # Create a dedicated queue for this table
            table_queue = asyncio.Queue(maxsize=1000)
            self.table_queues[table_name] = table_queue

            # Create a loader with its dedicated queue
            self.loaders[table_name] = BigQueryLoader(
                config, table_name, table_queue, dry_run=dry_run)

        if dry_run:
            logger.info(
                f"Initialized BigQueryLoaderManager with {len(self.loaders)} loaders in DRY-RUN mode")
        else:
            logger.info(
                f"Initialized BigQueryLoaderManager with {len(self.loaders)} loaders")

    async def start(self) -> None:
        """Start all BigQuery loaders."""
        logger.info("Starting all BigQuery loaders...")
        self._running = True

        try:
            # Start the router task that distributes rows to appropriate loaders
            router_task = asyncio.create_task(
                self._route_rows(), name="row_router")

            # Start all loaders concurrently
            loader_tasks = [asyncio.create_task(loader.start(), name=f"loader_{table}")
                            for table, loader in self.loaders.items()]

            # Combine all tasks
            all_tasks = [router_task] + loader_tasks

            try:
                await asyncio.gather(*all_tasks)
            except asyncio.CancelledError:
                logger.info("BigQuery loaders cancelled, shutting down...")
                # Cancel all tasks
                for task in all_tasks:
                    if not task.done():
                        task.cancel()
                # Wait for tasks to finish cancellation
                if all_tasks:
                    await asyncio.gather(*all_tasks, return_exceptions=True)
                raise

        except asyncio.CancelledError:
            logger.info("BigQuery loader manager cancelled")
            raise
        except Exception as e:
            logger.error(f"Error starting BigQuery loaders: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop all BigQuery loaders."""
        logger.info("Stopping all BigQuery loaders...")
        self._running = False

        # Stop all loaders concurrently
        tasks = [loader.stop() for loader in self.loaders.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _route_rows(self) -> None:
        """Route rows from the main queue to table-specific queues."""
        logger.info(
            f"Starting row router with queues for tables: {list(self.table_queues.keys())}")

        # Track routing statistics
        routed_counts = {table: 0 for table in self.table_queues}
        last_log_time = time.time()
        log_interval = 10  # Log more frequently for debugging

        while self._running:
            try:
                # Get a row from the main queue
                try:
                    row = await asyncio.wait_for(self.input_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # Log routing statistics periodically
                    current_time = time.time()
                    if current_time - last_log_time >= log_interval:
                        logger.info(f"Routing statistics: {routed_counts}")
                        # Also log queue sizes
                        queue_sizes = {table: queue.qsize()
                                       for table, queue in self.table_queues.items()}
                        logger.info(f"Table queue sizes: {queue_sizes}")
                        last_log_time = current_time
                    continue

                # Log every row for debugging
                # logger.info(
                #     f"Router received row for table: {row.target_table}, timestamp: {row.event_timestamp}")

                # Route to the appropriate table queue
                if row.target_table in self.table_queues:
                    # Put the row in the appropriate table queue
                    await self.table_queues[row.target_table].put(row)
                    routed_counts[row.target_table] += 1
                    # logger.info(
                    #     f"Successfully routed row to {row.target_table} queue (total: {routed_counts[row.target_table]})")
                else:
                    logger.warning(
                        f"No loader for table {row.target_table}, dropping row. Available tables: {list(self.table_queues.keys())}")

            except asyncio.CancelledError:
                logger.info(
                    f"Row router cancelled. Final routing counts: {routed_counts}")
                break
            except Exception as e:
                logger.error(f"Error routing row: {e}", exc_info=True)
                continue

        logger.info(f"Row router stopped. Total rows routed: {routed_counts}")

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from all loaders."""
        stats = {
            table_name: loader.get_stats()
            for table_name, loader in self.loaders.items()
        }

        # Add queue sizes
        for table_name, queue in self.table_queues.items():
            if table_name in stats:
                stats[table_name]["queue_size"] = queue.qsize()

        return stats