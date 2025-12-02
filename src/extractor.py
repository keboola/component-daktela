"""
Main extractor module for Daktela data extraction.

Handles extraction orchestration for both parallel and dependent tables.
"""

import asyncio
import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Set, TYPE_CHECKING

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
        server: str,
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
            server: Server name
            incremental: Whether to use incremental mode
            from_datetime: Start datetime for extraction
            to_datetime: End datetime for extraction
            requested_tables: List of table names to extract
            batch_size: Number of records to process in each batch (default: 10000)
        """
        self.api_client = api_client
        self.table_configs = table_configs
        self.component = component
        self.server = server
        self.incremental = incremental
        self.from_datetime = from_datetime
        self.to_datetime = to_datetime
        self.requested_tables = requested_tables
        self.batch_size = batch_size
        self.invalid_activity_ids: Set[str] = set()
        self._table_columns: Dict[str, List[str]] = {}

    async def extract_all(self):
        """Extract all requested tables."""
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

        # Separate into parallel and sequential tables
        parallel_tables = [t for t in tables_to_extract if self._should_extract_in_parallel(t)]
        sequential_tables = [t for t in tables_to_extract if not self._should_extract_in_parallel(t)]

        logging.info(
            f"Extracting {len(parallel_tables)} tables in parallel and {len(sequential_tables)} tables sequentially"
        )

        # Extract parallel tables first
        if parallel_tables:
            await self._extract_parallel_tables(parallel_tables)

        # Extract sequential tables (activities, activities_statuses, dependent tables)
        if sequential_tables:
            await self._extract_sequential_tables(sequential_tables)

        logging.info("Extraction completed successfully")

    def _should_extract_in_parallel(self, table_name: str) -> bool:
        """Check if this table can be extracted in parallel with others."""
        config = self.table_configs[table_name]
        has_requirements = len(config.get("requirements", [])) > 0
        is_activities = table_name in ["activities", "activities_statuses"]
        return not has_requirements and not is_activities

    def _is_dependent_table(self, table_name: str) -> bool:
        """Check if this table depends on a parent table."""
        config = self.table_configs[table_name]
        return bool(config.get("parent_table"))

    async def _extract_parallel_tables(self, tables: List[str]):
        """
        Extract multiple tables in parallel.

        Args:
            tables: List of table names to extract
        """
        logging.info(f"Extracting {len(tables)} tables in parallel")

        tasks = []
        for table_name in tables:
            tasks.append(self._extract_table(table_name))

        await asyncio.gather(*tasks)

    async def _extract_sequential_tables(self, tables: List[str]):
        """
        Extract tables sequentially (for activities and dependent tables).

        Args:
            tables: List of table names to extract
        """
        logging.info(f"Extracting {len(tables)} tables sequentially")

        for table_name in tables:
            await self._extract_table(table_name)

    async def _extract_table(self, table_name: str):
        """
        Extract a single table using batched processing for memory efficiency.

        Args:
            table_name: Name of table to extract
        """
        logging.info(f"Extracting table: {table_name}")

        table_config = self.table_configs[table_name]

        # Check if this is a dependent table
        if self._is_dependent_table(table_name):
            await self._extract_dependent_table(table_name)
            return

        # Build filters
        filters = self._build_filters(table_config)

        # Initialize transformer
        transformer = DataTransformer(self.server, table_name, table_config)

        # Table output name
        output_table_name = f"{self.server}_{table_name}.csv"

        # Fetch and process data in batches
        total_records = 0
        async for batch in self.api_client.fetch_table_data_batched(
            table_name=table_name,
            fields=table_config.get("fields", []),
            filters=filters,
            batch_size=self.batch_size,
        ):
            if not batch:
                continue

            # Transform batch
            transformed_records, invalid_ids = transformer.transform_records(batch)

            # Track invalid activity IDs
            if table_name == "activities" and invalid_ids:
                self.invalid_activity_ids.update(invalid_ids)

            if not transformed_records:
                continue

            total_records += self._write_records(output_table_name, table_config, transformed_records)

        # Finalize table (write manifest)
        if total_records > 0:
            self.component.finalize_table(output_table_name)
            logging.info(f"Completed extraction for table: {table_name} ({total_records} records)")
        else:
            logging.warning(f"No data found for table: {table_name}")

    async def _extract_dependent_table(self, table_name: str):
        """
        Extract a dependent table (child of parent table).

        Args:
            table_name: Name of dependent table to extract
        """
        table_config = self.table_configs[table_name]
        parent_table = table_config.get("parent_table")
        parent_id_field = table_config.get("parent_id_field", "id")

        logging.info(f"Extracting dependent table: {table_name} (parent: {parent_table})")

        # Read parent IDs from CSV
        parent_file = Path(self.component.tables_out_path) / f"{self.server}_{parent_table}.csv"
        parent_ids = self._read_column_values(parent_file, parent_id_field)

        if not parent_ids:
            logging.warning(f"No parent IDs found for dependent table: {table_name}")
            return

        # Filter out invalid activity IDs if parent is activities
        if parent_table == "activities":
            parent_ids = [pid for pid in parent_ids if pid not in self.invalid_activity_ids]
            logging.info(f"Filtered out {len(self.invalid_activity_ids)} invalid activity IDs")

        # Build filters
        filters = self._build_filters(table_config)

        # Initialize transformer
        transformer = DataTransformer(self.server, table_name, table_config)

        # Table output name
        output_table_name = f"{self.server}_{table_name}.csv"

        # Fetch and process data for each parent ID in batches
        batch = []
        total_records = 0

        for parent_id in parent_ids:
            records = await self.api_client.fetch_dependent_table_data(
                parent_table=parent_table,
                parent_id=parent_id,
                child_table=table_name,
                fields=table_config.get("fields", []),
                filters=filters,
            )

            if records:
                # Transform records
                transformed, _ = transformer.transform_records(records)
                batch.extend(transformed)
                total_records += len(transformed)

            # Write batch when it reaches threshold
            if len(batch) >= self.batch_size:
                self._write_records(output_table_name, table_config, batch)
                logging.debug(f"Wrote batch of {len(batch)} records for table {table_name}")
                batch = []

        # Write remaining records
        if batch:
            self._write_records(output_table_name, table_config, batch)
            logging.debug(f"Wrote final batch of {len(batch)} records for table {table_name}")

        # Finalize table (write manifest)
        if total_records > 0:
            self.component.finalize_table(output_table_name)
            logging.info(f"Completed extraction for dependent table {table_name}: {total_records} records")
        else:
            logging.warning(f"No data found for dependent table: {table_name}")

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
        # Start with server and id
        columns = ["server", "id"]

        # Add all other columns from sample record
        for key in sample_record.keys():
            if key not in columns:
                columns.append(key)

        return columns

    def _read_column_values(self, file_path: Path, column_name: str) -> List[str]:
        """Read non-empty values from a specific CSV column."""
        if not file_path.exists():
            logging.warning(f"CSV file not found: {file_path}")
            return []

        values: List[str] = []
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                value = row.get(column_name)
                if value:
                    values.append(value)

        logging.info(f"Read {len(values)} values from column {column_name} in {file_path}")
        return values

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
