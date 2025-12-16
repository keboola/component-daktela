"""
Daktela Extractor Component main class.
"""

import asyncio
import csv
import logging
import sys
import traceback
from datetime import datetime, timezone
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
        self.params: Configuration | None = None
        self._table_definitions: dict[str, Any] = {}
        self._schema_state: dict[str, Any] = {}

    def run(self) -> None:
        """Main execution - orchestrates the component workflow."""
        try:
            self.params = self._validate_and_get_configuration()

            # Load schema state from previous runs
            self._load_schema_state()

            # Run async extraction
            asyncio.run(self._run_async_extraction())

            # Save updated schema state
            self._save_schema_state()

            logging.info("Daktela extraction completed successfully")

        except UserException as err:
            logging.error(f"Configuration/API error: {err}")
            print(err, file=sys.stderr)
            sys.exit(1)

        except Exception:
            logging.exception("Unhandled error in component execution")
            traceback.print_exc(file=sys.stderr)
            sys.exit(2)

    def _load_schema_state(self) -> None:
        """Load schema state from previous runs."""
        state = self.get_state_file()
        self._schema_state = state.get("schema", {})
        if self._schema_state:
            logging.info(f"Loaded schema state for {len(self._schema_state)} endpoints")

    def _save_schema_state(self) -> None:
        """Save schema state for future runs."""
        state = self.get_state_file()
        state["schema"] = self._schema_state
        state["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.write_state_file(state)
        logging.info(f"Saved schema state for {len(self._schema_state)} endpoints")

    def get_schema_for_endpoint(self, endpoint: str) -> list[str] | None:
        """Get stored schema (columns) for an endpoint."""
        endpoint_schema = self._schema_state.get(endpoint)
        if endpoint_schema:
            return endpoint_schema.get("columns")
        return None

    def update_schema_for_endpoint(self, endpoint: str, columns: list[str]) -> None:
        """Update stored schema for an endpoint."""
        self._schema_state[endpoint] = {
            "columns": columns,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    @sync_action("listFields")
    def list_fields(self) -> dict[str, Any]:
        """
        Sync action to list all available fields for each endpoint.

        Returns a dictionary mapping endpoint names to their available fields.
        """
        self.params = self._validate_and_get_configuration()

        logging.info("Running listFields sync action")

        # Run async field discovery
        result = asyncio.run(self._discover_fields_async())

        logging.info(f"Discovered fields for {len(result)} endpoints")
        return result

    async def _discover_fields_async(self) -> dict[str, list[str]]:
        """Discover available fields for all requested endpoints."""
        params = self._require_params()
        result: dict[str, list[str]] = {}

        async with self._initialize_api_client() as api_client:
            for endpoint_config in params.data_selection.endpoints:
                endpoint = endpoint_config.endpoint
                try:
                    fields = await self._get_endpoint_fields(api_client, endpoint)
                    result[endpoint] = fields
                    logging.info(f"Discovered {len(fields)} fields for {endpoint}")
                except Exception as e:
                    logging.warning(f"Failed to discover fields for {endpoint}: {e}")
                    result[endpoint] = []

        return result

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
        # Use async context manager for API client (auth happens in __init__)
        async with self._initialize_api_client() as api_client:
            extractor = self._create_extractor(api_client)
            await extractor.extract_all()

    def _validate_and_get_configuration(self) -> Configuration:
        """Load and validate configuration parameters."""
        params = Configuration.from_dict(self.configuration.parameters)

        logging.info(f"Starting Daktela extraction from {params.connection.url}")
        logging.info(
            f"Date range: {params.data_selection.date_from} to {params.data_selection.date_to}"
        )
        logging.info(
            f"Endpoints to extract: {params.data_selection.get_endpoint_names()}"
        )
        logging.info(f"Incremental mode: {params.destination.incremental}")

        return params

    def _initialize_api_client(self) -> DaktelaApiClient:
        """Initialize and return configured API client (authenticates during init)."""
        params = self._require_params()
        return DaktelaApiClient(
            url=params.connection.url,
            username=params.connection.username,
            password=params.connection.password,
            max_concurrent=params.advanced.max_concurrent_requests,
            verify_ssl=params.connection.verify_ssl,
        )

    def _create_extractor(
        self,
        api_client: DaktelaApiClient,
    ) -> DaktelaExtractor:
        """Create and configure the extractor."""
        params = self._require_params()

        # Parse dates using keboola utils
        from_datetime = keboola.utils.get_past_date(
            params.data_selection.date_from
        ).strftime("%Y-%m-%d %H:%M:%S")
        to_datetime = keboola.utils.get_past_date(
            params.data_selection.date_to
        ).strftime("%Y-%m-%d %H:%M:%S")

        # Build table configs for each endpoint
        table_configs = {}
        for endpoint_config in params.data_selection.endpoints:
            endpoint = endpoint_config.endpoint
            # activitiesCall has different primary key
            if endpoint == "activitiesCall":
                table_configs[endpoint] = {"primary_keys": ["id_call"]}
            else:
                table_configs[endpoint] = {"primary_keys": ["name"]}

        return DaktelaExtractor(
            api_client=api_client,
            table_configs=table_configs,
            component=self,
            url=params.connection.url,
            requested_endpoints=params.data_selection.get_endpoint_names(),
            batch_size=params.advanced.batch_size,
            date_from=from_datetime,
            date_to=to_datetime,
            incremental=params.destination.incremental,
            max_concurrent_endpoints=params.advanced.max_concurrent_endpoints,
            configured_fields=params.data_selection.get_fields_dict(),
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

    def _require_params(self) -> Configuration:
        """Return initialized configuration or raise if missing."""
        if not self.params:
            raise UserException("Component parameters are not initialized.")
        return self.params


"""
Main entrypoint
"""
if __name__ == "__main__":
    comp = Component()
    # this triggers the run method by default and is controlled by the configuration.action parameter
    # Error handling is done in the run() method
    comp.execute_action()
