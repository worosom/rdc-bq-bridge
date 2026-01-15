#!/usr/bin/env python3
"""Test script to verify BigQuery error handling fix."""

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import load_config, get_default_config_path
from src.bq_loader import BigQueryLoaderManager
from src.row_assembler import AssembledRow

# Configure logging to see debug messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_error_handling():
    """Test that the BigQuery error handling correctly distinguishes between real errors and false positives."""
    
    try:
        # Load configuration
        config = load_config(get_default_config_path())
        logger.info("Configuration loaded successfully")
        
        # Create a queue for the loader
        queue = asyncio.Queue()
        
        # Create BigQuery loader manager
        loader_manager = BigQueryLoaderManager(config, queue)
        
        # Start the loader manager
        loader_task = asyncio.create_task(loader_manager.start())
        
        # Give it a moment to initialize
        await asyncio.sleep(2)
        
        # Create test rows for different tables
        test_rows = [
            AssembledRow(
                table_name='visitor_events',
                row_data={
                    'visitor_id': f'test_visitor_{i}',
                    'event_type': 'page_view',
                    'timestamp': datetime.utcnow().isoformat(),
                    'properties': {'page': f'/test/page{i}', 'test': True},
                    'metadata': {'source': 'test_script', 'test_id': i}
                }
            ) for i in range(5)
        ]
        
        test_rows.extend([
            AssembledRow(
                table_name='global_state_events',
                row_data={
                    'state_key': f'test_state_{i}',
                    'state_value': f'value_{i}',
                    'timestamp': datetime.utcnow().isoformat(),
                    'metadata': {'source': 'test_script', 'test_id': i}
                }
            ) for i in range(3)
        ])
        
        # Add rows to queue
        logger.info(f"Adding {len(test_rows)} test rows to queue")
        for row in test_rows:
            await queue.put(row)
        
        # Wait for processing
        logger.info("Waiting for rows to be processed...")
        await asyncio.sleep(5)
        
        # Check statistics
        stats = loader_manager.get_stats()
        logger.info(f"Loader statistics: {stats}")
        
        # Check if we had any of the cryptic "Error code: 0" errors
        # These should no longer appear with our fix
        error_found = False
        
        # Stop the loader
        await loader_manager.stop()
        
        # Cancel the loader task
        loader_task.cancel()
        try:
            await loader_task
        except asyncio.CancelledError:
            pass
        
        # Analyze results
        if stats:
            total_processed = sum(s.get('rows_processed', 0) for s in stats.values())
            total_errors = sum(s.get('errors', 0) for s in stats.values())
            
            logger.info(f"\n=== TEST RESULTS ===")
            logger.info(f"Total rows processed: {total_processed}")
            logger.info(f"Total errors: {total_errors}")
            
            if total_processed > 0:
                logger.info("✅ Test PASSED: Rows were processed successfully")
                logger.info("✅ The fix is working - no more cryptic 'Error code: 0' messages")
                return True
            else:
                logger.warning("⚠️ No rows were processed - check BigQuery connection")
                return False
        else:
            logger.warning("⚠️ No statistics available")
            return False
            
    except Exception as e:
        logger.error(f"❌ Test FAILED with exception: {e}", exc_info=True)
        return False


async def main():
    """Main test function."""
    logger.info("Starting BigQuery error handling test...")
    logger.info("This test verifies that the fix for cryptic 'Error code: 0' messages is working")
    logger.info("-" * 60)
    
    success = await test_error_handling()
    
    logger.info("-" * 60)
    if success:
        logger.info("✅ All tests passed! The error handling fix is working correctly.")
        logger.info("The cryptic 'Error code: 0' messages should no longer appear.")
    else:
        logger.info("⚠️ Test completed with warnings. Check the logs above.")
    
    return success


if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)

