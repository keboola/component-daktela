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
        """Extract all requested tables in two async batches."""
        logging.info(f"Starting extraction for {len(self.requested_tables)} tables")

        # Filter to only configured tables
        tables_to_extract = []
        for table_name in self.requested_tables:
            if table_name not in self.table_configs:
                logging.warning(
                    f"Table '{table_name}' not found in configuration. Skipping."
                )
                continue
            tables_to_extract.append(table_name)

        if not tables_to_extract:
            raise UserException("No valid tables to extract")

        # Auto-include parent tables for dependent tables
        tables_to_extract = self._auto_include_parent_tables(tables_to_extract)

        # Separate into parent/independent tables and dependent tables
        dependent_tables = []
        parent_and_independent_tables = []

        for table_name in tables_to_extract:
            if self._is_dependent_table(table_name):
                dependent_tables.append(table_name)
            else:
                parent_and_independent_tables.append(table_name)

        logging.info(
            f"First batch: {len(parent_and_independent_tables)} parent/independent tables. "
            f"Second batch: {len(dependent_tables)} dependent tables"
        )

        # First async batch: Extract parent and independent tables
        if parent_and_independent_tables:
            logging.info(
                f"Extracting parent/independent tables: {parent_and_independent_tables}"
            )
            await self._extract_batch(parent_and_independent_tables)

        # Second async batch: Extract dependent tables (after parents are complete)
        if dependent_tables:
            logging.info(f"Extracting dependent tables: {dependent_tables}")
            await self._extract_batch(dependent_tables)

        logging.info("Extraction completed successfully")

    def _get_table_endpoint(self, table_name: str, table_config: Dict[str, Any]) -> str:
        """Return endpoint override for table if configured."""
        return table_config.get("endpoint", table_name)

    def _get_child_endpoint(self, table_name: str, table_config: Dict[str, Any]) -> str:
        """Return endpoint override for child table if configured."""
        return table_config.get("child_endpoint", table_name)

    def _normalize_parent_ids(self, parent_ids: List[str], parent_table: str) -> List[str]:
        """
        Remove server/table prefixes and filter invalid IDs for dependent table calls.

        Args:
            parent_ids: Raw parent IDs read from CSV
            parent_table: Parent table name

        Returns:
            List of cleaned parent IDs
        """
        if not parent_ids:
            return []

        cleaned_ids = []
        server_prefix = f"{self.server}_"

        # First strip server prefix to align with API IDs and invalid tracking
        stripped_ids: List[str] = []
        filtered_out = 0
        for pid in parent_ids:
            without_server = pid[len(server_prefix) :] if pid.startswith(server_prefix) else pid

            if (
                parent_table == "activities"
                and self.invalid_activity_ids
                and (pid in self.invalid_activity_ids or without_server in self.invalid_activity_ids)
            ):
                filtered_out += 1
                continue

            stripped_ids.append(without_server)

        if filtered_out:
            logging.info(f"Filtered out {filtered_out} invalid activity IDs")

        # Then strip table-specific prefixes (plural and singular)
        prefixes: List[str] = [f"{parent_table}_"]
        if parent_table == "activities":
            prefixes.append("activity_")
        elif parent_table.endswith("ies"):
            prefixes.append(f"{parent_table[:-3]}y_")
        elif parent_table.endswith("s"):
            prefixes.append(f"{parent_table[:-1]}_")

        logging.debug(f"Will try to strip prefixes: {prefixes}")

        for pid in stripped_ids:
            cleaned = pid
            for prefix in prefixes:
                if cleaned.startswith(prefix):
                    cleaned = cleaned.replace(prefix, "", 1)
                    break
            cleaned_ids.append(cleaned)

        logging.debug(
            f"Cleaned {len(cleaned_ids)} parent IDs (removed server/table prefixes)"
        )
        if cleaned_ids:
            logging.debug(f"First 3 cleaned parent IDs: {cleaned_ids[:3]}")

        return cleaned_ids

    def _is_dependent_table(self, table_name: str) -> bool:
        """Check if this table depends on a parent table."""
        config = self.table_configs[table_name]
        return bool(config.get("parent_table"))

    def _auto_include_parent_tables(self, tables: List[str]) -> List[str]:
        """
        Auto-include parent tables for dependent tables if not already selected.

        Args:
            tables: List of requested table names

        Returns:
            Updated list with parent tables included
        """
        tables_set = set(tables)
        added_parents = []

        for table_name in tables:
            if self._is_dependent_table(table_name):
                config = self.table_configs[table_name]
                parent_table = config.get("parent_table")

                if parent_table and parent_table not in tables_set:
                    # Check if parent table is configured
                    if parent_table in self.table_configs:
                        tables_set.add(parent_table)
                        added_parents.append(parent_table)
                        logging.info(
                            f"Auto-including parent table '{parent_table}' required by dependent table '{table_name}'"
                        )
                    else:
                        logging.warning(
                            f"Parent table '{parent_table}' required by '{table_name}' "
                            f"is not configured in table_definitions.json"
                        )

        return list(tables_set)

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
        # Force full-field extraction regardless of config
        table_config_all_fields = {**table_config, "fields": []}

        # Check if this is a dependent table
        if self._is_dependent_table(table_name):
            await self._extract_dependent_table(table_name)
            return

        # Build filters
        filters = self._build_filters(table_config_all_fields)

        # Endpoint override support
        endpoint = self._get_table_endpoint(table_name, table_config_all_fields)

        # Initialize transformer
        transformer = DataTransformer(self.server, table_name, table_config_all_fields)

        # Table output name
        output_table_name = f"{self.server}_{table_name}.csv"

        # Fetch and process data in batches
        total_records = 0
        async for batch in self.api_client.fetch_table_data_batched(
            table_name=table_name,
            endpoint=endpoint,
            fields=[],
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

            total_records += self._write_records(
                output_table_name, table_config_all_fields, transformed_records
            )

        # Finalize table (write manifest)
        if total_records > 0:
            self.component.finalize_table(output_table_name)
            logging.info(
                f"Completed extraction for table: {table_name} ({total_records} records)"
            )
        else:
            logging.warning(f"No data found for table: {table_name}")

    async def _extract_dependent_table(self, table_name: str):
        """
        Extract a dependent table (child of parent table).

        Args:
            table_name: Name of dependent table to extract
        """
        table_config = self.table_configs[table_name]
        table_config_all_fields = {**table_config, "fields": []}
        parent_table = table_config.get("parent_table")
        parent_id_field = table_config.get("parent_id_field", "id")

        logging.info(
            f"Extracting dependent table: {table_name} (parent: {parent_table}, parent_id_field: {parent_id_field})"
        )
        child_endpoint = self._get_child_endpoint(table_name, table_config_all_fields)
        parent_table_config = self.table_configs.get(parent_table, {})
        parent_endpoint = self._get_table_endpoint(parent_table, parent_table_config)

        # Read parent IDs from CSV
        parent_file = (
            Path(self.component.tables_out_path) / f"{self.server}_{parent_table}.csv"
        )
        logging.debug(f"Looking for parent file: {parent_file}")
        logging.debug(f"Parent file exists: {parent_file.exists()}")

        if parent_file.exists():
            # Read a few lines to debug
            with open(parent_file, "r", encoding="utf-8") as f:
                lines = f.readlines()[:3]
                logging.debug(f"Parent file first 3 lines: {lines}")

        parent_ids = self._read_column_values(parent_file, parent_id_field)
        logging.debug(
            f"Read {len(parent_ids)} parent IDs from column '{parent_id_field}'"
        )
        if parent_ids:
            logging.debug(f"First 3 raw parent IDs: {parent_ids[:3]}")

        if not parent_ids:
            logging.warning(f"No parent IDs found for dependent table: {table_name}")
            return

        parent_ids = self._normalize_parent_ids(parent_ids, parent_table)
        if not parent_ids:
            logging.warning(
                f"No valid parent IDs found after normalization for dependent table: {table_name}"
            )
            return

        # Don't use date filters for dependent tables
        # The parent table was already filtered by date, so children will inherit that filtering
        filters = {}

        # Initialize transformer
        transformer = DataTransformer(self.server, table_name, table_config_all_fields)

        # Table output name
        output_table_name = f"{self.server}_{table_name}.csv"

        # Fetch and process data for each parent ID in batches
        batch = []
        total_records = 0

        for parent_id in parent_ids:
            try:
                records = await self.api_client.fetch_dependent_table_data(
                    parent_table=parent_table,
                    parent_id=parent_id,
                    child_table=table_name,
                    fields=[],
                    filters=filters,
                    parent_endpoint=parent_endpoint,
                    child_endpoint=child_endpoint,
                )

                if records:
                    # Transform records
                    transformed, _ = transformer.transform_records(records)
                    batch.extend(transformed)
                    total_records += len(transformed)

            except Exception as e:
                logging.warning(
                    f"Failed to fetch dependent table {table_name} for parent {parent_table}:{parent_id}: {str(e)}"
                )
                # Continue with next parent ID instead of failing entire extraction
                continue

            # Write batch when it reaches threshold
            if len(batch) >= self.batch_size:
                self._write_records(output_table_name, table_config_all_fields, batch)
                logging.debug(
                    f"Wrote batch of {len(batch)} records for table {table_name}"
                )
                batch = []

        # Write remaining records
        if batch:
            self._write_records(output_table_name, table_config_all_fields, batch)
            logging.debug(
                f"Wrote final batch of {len(batch)} records for table {table_name}"
            )

        # Finalize table (write manifest)
        if total_records > 0:
            self.component.finalize_table(output_table_name)
            logging.info(
                f"Completed extraction for dependent table {table_name}: {total_records} records"
            )
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

        logging.info(
            f"Read {len(values)} values from column {column_name} in {file_path}"
        )
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
