"""
Daktela Extractor Component main class.
"""

import asyncio
import csv
import logging
import os
import sys
import traceback
from typing import Any

import keboola.utils
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException

from configuration import Configuration
from daktela_client import DaktelaApiClient
from extractor import DaktelaExtractor


class Component(ComponentBase):
    """
    Daktela Extractor Component.

    Extracts data from Daktela CRM/Contact Center API v6 and produces CSV outputs
    compatible with Keboola storage.
    """

    def __init__(self) -> None:
        super().__init__()
        self.config: Configuration | None = None
        self._table_definitions: dict[str, Any] = {}

    def run(self) -> None:
        """Main execution - orchestrates the component workflow."""
        try:
            # Load and validate merged configuration (root + row merged by platform)
            self.config = self._load_configuration()

            # Run async extraction
            asyncio.run(self._run_async_extraction())

            logging.info("Daktela extraction completed successfully")

        except UserException as err:
            logging.error(f"Configuration/API error: {err}")
            print(err, file=sys.stderr)
            sys.exit(1)

        except Exception:
            logging.exception("Unhandled error in component execution")
            traceback.print_exc(file=sys.stderr)
            sys.exit(2)

    @sync_action("testConnection")
    def test_connection(self) -> dict[str, str]:
        """
        Sync action to test connection to Daktela API.

        Validates credentials by attempting to authenticate with the API.
        Returns success/error status with a message.
        """
        try:
            # Get connection parameters
            params = self.configuration.parameters
            connection = params.get("connection", {})
            url = connection.get("url")
            username = connection.get("username")
            password = connection.get("#password")
            verify_ssl = connection.get("verify_ssl", True)

            if not url:
                raise UserException("URL is required")
            if not username:
                raise UserException("Username is required")
            if not password:
                raise UserException("Password is required")

            logging.info(f"Testing connection to {url}")

            # Test connection by attempting to authenticate
            result = asyncio.run(
                self._test_connection_async(url, username, password, verify_ssl)
            )

            return result

        except UserException as e:
            return {"status": "error", "message": str(e)}
        except Exception as e:
            logging.exception("Connection test failed")
            return {"status": "error", "message": f"Connection failed: {e}"}

    async def _test_connection_async(
        self, url: str, username: str, password: str, verify_ssl: bool
    ) -> dict[str, str]:
        """Test connection to Daktela API asynchronously."""
        api_client = DaktelaApiClient(
            url=url,
            username=username,
            password=password,
            max_concurrent=1,
            verify_ssl=verify_ssl,
        )
        async with api_client:
            # If we get here, authentication was successful
            return {"status": "success", "message": "Connection successful"}

    @sync_action("listFields")
    def list_fields(self) -> dict[str, Any]:
        """
        Sync action to list available fields for the current row's endpoint.

        Platform passes merged config (root + row) in configuration.parameters.
        Returns a dictionary with the endpoint name and its available fields.
        """
        # Load merged configuration (root + row fields)
        try:
            config = Configuration.from_dict(self.configuration.parameters)
        except Exception as e:
            logging.error(f"Failed to load configuration for sync action: {e}")
            return {"error": f"Invalid configuration: {e}"}

        # Store for use by API client initialization
        self.config = config

        logging.info(f"Running listFields sync action for endpoint: {config.endpoint}")

        # Run async field discovery for this endpoint
        result = asyncio.run(self._discover_fields_async(config.endpoint))

        logging.info(f"Discovered {len(result)} fields for {config.endpoint}")
        return {config.endpoint: result}

    async def _discover_fields_async(self, endpoint: str) -> list[str]:
        """Discover available fields for a single endpoint."""
        async with self._initialize_api_client() as api_client:
            try:
                fields = await self._get_endpoint_fields(api_client, endpoint)
                logging.info(f"Discovered {len(fields)} fields for {endpoint}")
                return fields
            except Exception as e:
                logging.warning(f"Failed to discover fields for {endpoint}: {e}")
                return []

    async def _get_endpoint_fields(
        self, api_client: DaktelaApiClient, endpoint: str
    ) -> list[str]:
        """Get available fields for a single endpoint by fetching a sample record."""
        # Fetch just one record to discover fields
        async for page in api_client.fetch_table_data_batched(
            table_name=endpoint,
            endpoint=endpoint,
            batch_size=1,
        ):
            if page and len(page) > 0:
                # Extract field names from the first record
                return sorted(page[0].keys())
            break

        return []

    async def _run_async_extraction(self) -> None:
        """Run the async extraction process."""
        if not self.config:
            raise UserException("No configuration available for extraction")

        # Use async context manager for API client (auth happens in __init__)
        async with self._initialize_api_client() as api_client:
            logging.info(f"Processing endpoint: {self.config.endpoint}")
            extractor = self._create_extractor(api_client)
            await extractor.extract_all()

    def _load_configuration(self) -> Configuration:
        """
        Load and validate merged configuration.

        Platform merges root config (from configSchema.json) with row config
        (from configRowSchema.json) into self.configuration.parameters before
        running the component.
        """
        config = Configuration.from_dict(self.configuration.parameters)

        # Validate that required fields for extraction are present
        config.validate_for_extraction()

        logging.info(
            f"Starting Daktela extraction from {config.connection.url}, "
            f"endpoint={config.endpoint}, "
            f"date_from={config.date_from}, date_to={config.date_to}, "
            f"load_type={config.destination.load_type}"
        )

        return config

    def _initialize_api_client(self) -> DaktelaApiClient:
        """Initialize and return configured API client (authenticates during init)."""
        config = self._require_config()
        return DaktelaApiClient(
            url=config.connection.url,
            username=config.connection.username,
            password=config.connection.password,
            max_concurrent=config.advanced.max_concurrent_requests,
            verify_ssl=config.connection.verify_ssl,
        )

    def _create_extractor(
        self,
        api_client: DaktelaApiClient,
    ) -> DaktelaExtractor:
        """Create and configure the extractor."""
        config = self._require_config()

        # Parse dates using keboola utils
        from_datetime = keboola.utils.get_past_date(config.date_from).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        to_datetime = keboola.utils.get_past_date(config.date_to).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Build table config for this endpoint
        endpoint = config.endpoint
        table_configs = {}

        # Use primary_key from config if set, otherwise use defaults
        if config.destination.primary_key:
            primary_keys = config.destination.primary_key
        elif endpoint == "activitiesCall":
            primary_keys = ["id_call"]
        else:
            primary_keys = ["name"]

        table_configs[endpoint] = {"primary_keys": primary_keys}

        # Prepare configured fields dict (only for this endpoint)
        configured_fields = {}
        if config.fields:
            configured_fields[endpoint] = config.fields

        return DaktelaExtractor(
            api_client=api_client,
            table_configs=table_configs,
            component=self,
            url=config.connection.url,
            requested_endpoints=[endpoint],
            batch_size=config.advanced.batch_size,
            date_from=from_datetime,
            date_to=to_datetime,
            incremental=config.destination.incremental,
            configured_fields=configured_fields if configured_fields else None,
        )

    def write_table_data(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        table_config: dict[str, Any],
        columns: list[str],
        incremental: bool = False,
    ) -> None:
        """
        Write table data using create_out_table_definition and write_manifest pattern.

        Args:
            table_name: Name of the output table (e.g., "server_tablename.csv")
            records: List of records to write
            table_config: Table configuration dict
            incremental: Whether to use incremental mode
            columns: List of column names
        """
        table_definitions = self._get_table_definitions()

        # Create table definition on first write
        if table_name not in table_definitions:
            out_table = self.create_out_table_definition(
                table_name,
                columns=columns,
                primary_key=table_config.get("primary_keys"),
                incremental=incremental,
                has_header=True,
            )

            table_definitions[table_name] = out_table

            # Write header
            with open(out_table.full_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()

            logging.info(
                f"Created table definition for {table_name} with {len(columns)} columns"
            )

        # Get table definition
        out_table = table_definitions.get(table_name)

        if not out_table:
            raise UserException(
                f"Table definition not found for {table_name}. This should not happen."
            )

        # Append records
        if records:
            with open(out_table.full_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
                for record in records:
                    # Ensure all columns are present
                    row = {col: record.get(col) for col in columns}
                    writer.writerow(row)

            logging.info(f"Wrote {len(records)} records to {table_name}")

    def rewrite_table_columns(
        self,
        table_name: str,
        columns: list[str],
    ) -> None:
        """Rewrite an existing CSV file with an extended column list.

        Streams the already-written rows through a temporary file and then
        atomically replaces the original via ``os.replace``.  This keeps memory
        usage proportional to a single row (rather than the whole table) and
        guarantees the original file survives any mid-write failure -- a partial
        temp file is simply discarded and the original is left untouched.
        Existing rows receive empty strings for the newly added columns.

        Args:
            table_name: Name of the output table (e.g., "tickets.csv")
            columns: The full (extended) column list
        """
        out_table = self._get_table_definitions().get(table_name)
        if not out_table:
            raise UserException(
                f"Cannot rewrite columns: no table definition for {table_name}."
            )

        tmp_path = f"{out_table.full_path}.tmp"
        with open(out_table.full_path, "r", newline="", encoding="utf-8") as src, \
                open(tmp_path, "w", newline="", encoding="utf-8") as dst:
            reader = csv.DictReader(src)
            writer = csv.DictWriter(dst, fieldnames=columns)
            writer.writeheader()
            for row in reader:
                writer.writerow({col: row.get(col, "") for col in columns})

        os.replace(tmp_path, out_table.full_path)
        # Update the table definition so the manifest reflects all columns
        out_table.columns = columns
        logging.info(f"Rewrote {table_name} with {len(columns)} columns")

    def finalize_table(self, table_name: str) -> None:
        """
        Finalize table by writing manifest.

        Args:
            table_name: Name of the output table
        """
        out_table = self._get_table_definitions().get(table_name)

        if out_table:
            self.write_manifest(out_table)
            logging.info(f"Wrote manifest for {table_name}")
        else:
            logging.warning(
                f"No table definition found for {table_name}, skipping manifest"
            )

    def _get_table_definitions(self) -> dict[str, Any]:
        """Return initialized table definitions container."""
        if not hasattr(self, "_table_definitions"):
            self._table_definitions = {}
        return self._table_definitions

    def _require_config(self) -> Configuration:
        """Return initialized configuration or raise if missing."""
        if not self.config:
            raise UserException("Component configuration is not initialized.")
        return self.config


"""
Main entrypoint
"""
if __name__ == "__main__":
    comp = Component()
    # this triggers the run method by default and is controlled by the configuration.action parameter
    # Error handling is done in the run() method
    comp.execute_action()
