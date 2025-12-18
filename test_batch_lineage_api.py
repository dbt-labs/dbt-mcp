#!/usr/bin/env python
"""
Test script to verify batch fetching optimization with real Discovery API.
This script runs a lineage query and counts the actual API calls made.

Usage:
    source .env && python test_batch_lineage_api.py
"""

import asyncio
import logging
import os

from dbt_mcp.config.settings import DbtMcpSettings, CredentialsProvider
from dbt_mcp.config.config_providers import DefaultDiscoveryConfigProvider
from dbt_mcp.discovery.client import LineageFetcher, LineageDirection, MetadataAPIClient


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class APICallCounter:
    """Wrapper to count API calls."""

    def __init__(self, client):
        self.client = client
        self.call_count = 0
        self.calls = []

    @property
    def config_provider(self):
        """Pass through config_provider."""
        return self.client.config_provider

    async def execute_query(self, query, variables=None):
        """Execute query and count the call."""
        self.call_count += 1

        # Log the call
        query_type = "BATCH" if "batch_" in query else "INDIVIDUAL"
        logger.info(f"  API Call #{self.call_count}: {query_type}")

        # Store call info
        self.calls.append(
            {
                "number": self.call_count,
                "type": query_type,
            }
        )

        # Execute the actual query
        result = await self.client.execute_query(query, variables)
        return result


async def test_lineage_with_real_api():
    """Test lineage fetching with real API and count calls."""

    logger.info("=" * 80)
    logger.info("Testing Batch Lineage Optimization with Real Discovery API")
    logger.info("=" * 80)

    # Create settings from environment (reads from .env)
    settings = DbtMcpSettings()

    # Create credentials provider
    credentials_provider = CredentialsProvider(settings)

    # Create discovery config provider
    config_provider = DefaultDiscoveryConfigProvider(credentials_provider)

    # Create API client
    api_client = MetadataAPIClient(config_provider)

    # Wrap client to count calls
    counter = APICallCounter(api_client)

    # Create lineage fetcher with wrapped client
    lineage_fetcher = LineageFetcher(api_client=counter)

    # Test model unique ID - update this to a model in your project
    test_model = os.getenv("TEST_MODEL_UNIQUE_ID", "model.jaffle_shop.customers")

    logger.info(f"\nFetching lineage for: {test_model}")
    logger.info("Direction: BOTH (ancestors + descendants)")
    logger.info("-" * 80)

    try:
        # Fetch lineage
        result = await lineage_fetcher.fetch_lineage(
            unique_id=test_model,
            types=[],
            direction=LineageDirection.BOTH,
        )

        logger.info("-" * 80)
        logger.info("\nRESULTS:")
        logger.info(f"  Target: {result['target']['name']}")
        logger.info(f"  Ancestors: {len(result['ancestors'])} nodes")
        logger.info(f"  Descendants: {len(result['descendants'])} nodes")
        logger.info(
            f"  Total nodes in lineage: {len(result['ancestors']) + len(result['descendants'])}"
        )
        logger.info(f"\n  Total API Calls: {counter.call_count}")

        # Calculate expected calls without batching
        # Without batching: 1 initial + 1 per ancestor + 1 per descendant
        total_nodes = len(result["ancestors"]) + len(result["descendants"])

        # With level-based BFS, we need to estimate based on depth
        # For a rough estimate, assume worst case of sequential fetching
        sequential_calls_worst = 1 + total_nodes  # Initial + 1 per node

        logger.info("\nPERFORMANCE COMPARISON:")
        logger.info(f"  With Batching: {counter.call_count} calls")
        logger.info(
            f"  Without Batching (estimated worst case): {sequential_calls_worst} calls"
        )

        if counter.call_count < sequential_calls_worst:
            reduction = (
                (sequential_calls_worst - counter.call_count) / sequential_calls_worst
            ) * 100
            logger.info(f"  Reduction: ~{reduction:.1f}%")
            logger.info("  ✅ Batching optimization is working!")
        else:
            logger.info("  Result: No improvement over worst case")

        logger.info("\nAPI CALL BREAKDOWN:")
        batch_calls = sum(1 for call in counter.calls if call["type"] == "BATCH")
        individual_calls = sum(
            1 for call in counter.calls if call["type"] == "INDIVIDUAL"
        )
        logger.info(f"  Batch queries: {batch_calls}")
        logger.info(f"  Individual queries: {individual_calls}")

        logger.info("\n" + "=" * 80)
        logger.info("Test completed successfully!")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"\nError during test: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(test_lineage_with_real_api())
