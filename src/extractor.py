"""Main extractor module for Daktela data extraction."""

import asyncio
import json
import logging
import os
from typing import Any, TYPE_CHECKING

from configuration import DEFAULT_BATCH_SIZE, DEFAULT_MAX_CONCURRENT_ENDPOINTS
from daktela_client import DaktelaApiClient
from transformer import DataTransformer
from keboola.component.exceptions import UserException

if TYPE_CHECKING:
    from component import Component


class DaktelaExtractor:
    """Main extractor class that orchestrates data extraction."""

    def __init__(
        self,
        api_client: DaktelaApiClient,
        table_configs: dict[str, Any],
        component: "Component",
        url: str,
        requested_endpoints: list[str],
        batch_size: int = DEFAULT_BATCH_SIZE,
        date_from: str | None = None,
        date_to: str | None = None,
        incremental: bool = False,
        max_concurrent_endpoints: int = DEFAULT_MAX_CONCURRENT_ENDPOINTS,
    ):
        """
        Initialize extractor.

        Args:
            api_client: Configured API client
            table_configs: Dictionary of table configurations
            component: Component instance for writing tables
            url: Base URL (e.g., https://customer.daktela.com)
            requested_endpoints: List of endpoint names to extract
            batch_size: Number of records to process in each batch (default: 1000)
            date_from: Start date for filtering (for supported endpoints)
            date_to: End date for filtering (for supported endpoints)
            incremental: Whether to use incremental mode
            max_concurrent_endpoints: Maximum number of endpoints to extract concurrently
        """
        self.api_client = api_client
        self.table_configs = table_configs
        self.component = component
        self.url = url
        self.requested_endpoints = requested_endpoints
        self.batch_size = batch_size
        self.date_from = date_from
        self.date_to = date_to
        self.incremental = incremental
        self.max_concurrent_endpoints = max_concurrent_endpoints
        self._table_columns: dict[str, list[str]] = {}
        self._column_definitions = self._load_column_definitions()

    async def extract_all(self):
        """
        Extract all requested endpoints asynchronously.

        Uses two-phase extraction like the old component:
        Phase 1: Extract all tables except activities and activities_statuses
        Phase 2: Extract activities and activities_statuses

        This is done because activities may depend on data from other tables
        for filtering invalid activities.
        """
        logging.info(f"Starting extraction for {len(self.requested_endpoints)} endpoints")

        if not self.requested_endpoints:
            raise UserException("No endpoints specified for extraction")

        # Define activities-related endpoints that need special treatment
        # These are extracted in phase 2 after all other tables
        activities_endpoints = {
            "activities",
            "activities_statuses",
            "activitiesCall",
            "activitiesChat",
            "activitiesEmail",
        }

        # Phase 1: Extract all tables except activities-related ones
        phase1_endpoints = [ep for ep in self.requested_endpoints if ep not in activities_endpoints]
        if phase1_endpoints:
            logging.info(f"Phase 1: Extracting {len(phase1_endpoints)} endpoints "
                         f"(max {self.max_concurrent_endpoints} concurrent)")
            await self._run_endpoints_with_limit(phase1_endpoints)
            logging.info("Phase 1 extraction completed")

        # Phase 2: Extract activities-related tables
        phase2_endpoints = [ep for ep in self.requested_endpoints if ep in activities_endpoints]
        if phase2_endpoints:
            logging.info(f"Phase 2: Extracting {len(phase2_endpoints)} activities-related endpoints "
                         f"(max {self.max_concurrent_endpoints} concurrent)")
            await self._run_endpoints_with_limit(phase2_endpoints)
            logging.info("Phase 2 extraction completed")

        logging.info("Extraction completed successfully")

    async def _run_endpoints_with_limit(self, endpoints: list[str]) -> None:
        """
        Run endpoint extractions with bounded concurrency.

        This limits the number of endpoints being extracted simultaneously
        to prevent OOM when multiple large tables are extracted at once.

        Args:
            endpoints: List of endpoint names to extract
        """
        sem = asyncio.Semaphore(self.max_concurrent_endpoints)

        async def run_one(endpoint: str) -> None:
            async with sem:
                await self._extract_table(endpoint)

        await asyncio.gather(*(run_one(ep) for ep in endpoints))

    def _load_column_definitions(self) -> dict[str, list[str]]:
        """Load column definitions from table-columns.json."""
        columns_file = os.path.join(os.path.dirname(__file__), "table-columns.json")
        try:
            with open(columns_file, "r") as f:
                data = json.load(f)
                # Return only the top-level mapping (endpoint -> columns array)
                return {k: v for k, v in data.items() if isinstance(v, list)}
        except Exception as e:
            logging.warning(f"Could not load table-columns.json: {e}")
            return {}

    def _get_table_endpoint(self, table_name: str, table_config: dict[str, Any]) -> str:
        """Return endpoint override for table if configured."""
        return table_config.get("endpoint", table_name)

    async def _extract_table(self, table_name: str):
        """
        Extract a single table using batched processing for memory efficiency.

        Args:
            table_name: Name of table to extract
        """
        logging.info(f"Extracting table: {table_name}")

        table_config = self.table_configs[table_name]
        write_batch_size = max(1, self.batch_size)

        # Endpoint override support
        endpoint = self._get_table_endpoint(table_name, table_config)

        # Initialize transformer
        transformer = DataTransformer(table_name, table_config)

        # Table output name
        output_table_name = f"{table_name}.csv"

        # Get fields to fetch from column definitions
        fields = self._column_definitions.get(table_name)

        # Fetch and process data in pages
        total_records = 0
        async for page in self.api_client.fetch_table_data_batched(
            table_name=table_name,
            endpoint=endpoint,
            date_from=self.date_from,
            date_to=self.date_to,
            batch_size=self.batch_size,
            fields=fields,
        ):
            if not page:
                continue

            # Transform page records one by one and write in small batches
            write_batch = []
            for transformed_record in transformer.transform_records(page):
                write_batch.append(transformed_record)

                # Write in configurable batches to reduce memory footprint
                if len(write_batch) >= write_batch_size:
                    total_records += self._write_records(output_table_name, table_config, write_batch)
                    write_batch = []

            # Write remaining records from this page
            if write_batch:
                total_records += self._write_records(output_table_name, table_config, write_batch)

        # Finalize table (write manifest)
        if total_records > 0:
            self.component.finalize_table(output_table_name)
            logging.info(f"Completed extraction for table: {table_name} ({total_records} records)")
        else:
            logging.warning(f"No data found for table: {table_name}")

    def _get_columns(self, sample_record: dict[str, Any]) -> list[str]:
        """
        Get ordered list of columns for output.

        Args:
            sample_record: Sample record to extract columns from

        Returns:
            Ordered list of column names
        """
        # Start with id
        columns = ["id"]

        # Add all other columns from sample record
        for key in sample_record.keys():
            if key not in columns:
                columns.append(key)

        return columns

    def _write_records(
        self,
        output_table_name: str,
        table_config: dict[str, Any],
        records: list[dict[str, Any]],
    ) -> int:
        """Write a batch of records via the component and return written count."""
        if not records:
            return 0

        if output_table_name not in self._table_columns:
            self._table_columns[output_table_name] = self._get_columns(records[0])

        self.component.write_table_data(
            table_name=output_table_name,
            records=records,
            table_config=table_config,
            columns=self._table_columns[output_table_name],
            incremental=self.incremental,
        )

        return len(records)
