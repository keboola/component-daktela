"""Main extractor module for Daktela data extraction."""

import asyncio
import logging
from typing import Any, Dict, List, TYPE_CHECKING

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
        table_configs: Dict[str, Any],
        component: "Component",
        url: str,
        incremental: bool,
        from_datetime: str,
        to_datetime: str,
        requested_tables: List[str],
        batch_size: int = 10000,
    ):
        """
        Initialize extractor.

        Args:
            api_client: Configured API client
            table_configs: Dictionary of table configurations
            component: Component instance for writing tables
            url: Base URL (e.g., https://customer.daktela.com)
            incremental: Whether to use incremental mode
            from_datetime: Start datetime for extraction
            to_datetime: End datetime for extraction
            requested_tables: List of table names to extract
            batch_size: Number of records to process in each batch (default: 10000)
        """
        self.api_client = api_client
        self.table_configs = table_configs
        self.component = component
        self.url = url
        self.incremental = incremental
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime
        self.requested_tables = requested_tables
        self.batch_size = batch_size
        self._table_columns: Dict[str, List[str]] = {}

    async def extract_all(self):
        """Extract all requested tables asynchronously."""
        logging.info(f"Starting extraction for {len(self.requested_tables)} tables")

        # Filter to only configured tables
        tables_to_extract = []
        for table_name in self.requested_tables:
            if table_name not in self.table_configs:
                logging.warning(f"Table '{table_name}' not found in configuration. Skipping.")
                continue
            tables_to_extract.append(table_name)

        if not tables_to_extract:
            raise UserException("No valid tables to extract")

        await self._extract_batch(tables_to_extract)

        logging.info("Extraction completed successfully")

    def _get_table_endpoint(self, table_name: str, table_config: Dict[str, Any]) -> str:
        """Return endpoint override for table if configured."""
        return table_config.get("endpoint", table_name)

    async def _extract_batch(self, tables: List[str]):
        """
        Extract multiple tables asynchronously in parallel.

        Args:
            tables: List of table names to extract
        """
        logging.info(f"Extracting {len(tables)} tables asynchronously")

        tasks = []
        for table_name in tables:
            tasks.append(self._extract_table(table_name))

        await asyncio.gather(*tasks)

    async def _extract_table(self, table_name: str):
        """
        Extract a single table using batched processing for memory efficiency.

        Args:
            table_name: Name of table to extract
        """
        logging.info(f"Extracting table: {table_name}")

        table_config = self.table_configs[table_name]

        # Build filters
        filters = self._build_filters(table_config)

        # Endpoint override support
        endpoint = self._get_table_endpoint(table_name, table_config)

        # Initialize transformer
        transformer = DataTransformer(table_name, table_config)

        # Table output name
        output_table_name = f"{table_name}.csv"

        # Fetch and process data in batches
        total_records = 0
        async for batch in self.api_client.fetch_table_data_batched(
            table_name=table_name,
            endpoint=endpoint,
            filters=filters,
            batch_size=self.batch_size,
        ):
            if not batch:
                continue

            # Transform batch
            transformed_records, _ = transformer.transform_records(batch)

            if not transformed_records:
                continue

            total_records += self._write_records(output_table_name, table_config, transformed_records)

        # Finalize table (write manifest)
        if total_records > 0:
            self.component.finalize_table(output_table_name)
            logging.info(f"Completed extraction for table: {table_name} ({total_records} records)")
        else:
            logging.warning(f"No data found for table: {table_name}")

    def _build_filters(self, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build filters for API request.

        Args:
            table_config: Table configuration dict

        Returns:
            Dictionary of filters
        """
        filters = {}

        # Add date range filters
        filters["from"] = self.from_datetime
        filters["to"] = self.to_datetime

        # Add table-specific filters from config
        config_filters = table_config.get("filters", {})
        if config_filters:
            filters.update(config_filters)

        return filters

    def _get_columns(self, sample_record: Dict[str, Any]) -> List[str]:
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
        table_config: Dict[str, Any],
        records: List[Dict[str, Any]],
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
            incremental=self.incremental,
            columns=self._table_columns[output_table_name],
        )

        return len(records)
