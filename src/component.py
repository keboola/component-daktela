"""
Daktela Extractor Component main class.
"""

import asyncio
import csv
import logging
import sys
import traceback
import keboola.utils
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from keboola.component.base import ComponentBase
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
        self.params: Optional[Configuration] = None
        self._table_definitions: Dict[str, Any] = {}

    def run(self) -> None:
        """Main execution - orchestrates the component workflow."""
        try:
            self.params = self._validate_and_get_configuration()

            # Apply state for incremental processing
            self._apply_state()

            # Run async extraction
            asyncio.run(self._run_async_extraction())

            # Save state after successful extraction
            self._save_state()

            logging.info("Daktela extraction completed successfully")

        except UserException as err:
            logging.error(f"Configuration/API error: {err}")
            print(err, file=sys.stderr)
            sys.exit(1)

        except Exception:
            logging.exception("Unhandled error in component execution")
            traceback.print_exc(file=sys.stderr)
            sys.exit(2)

    async def _run_async_extraction(self) -> None:
        """Run the async extraction process."""
        # Use async context manager for API client (auth happens in __init__)
        async with self._initialize_api_client() as api_client:
            extractor = self._create_extractor(api_client)
            await extractor.extract_all()

    def _validate_and_get_configuration(self) -> Configuration:
        """Load and validate configuration parameters."""
        params = Configuration(**self.configuration.parameters)

        logging.info(f"Starting Daktela extraction from {params.connection.url}")
        logging.info(f"Date range: {params.data_selection.date_from} to {params.data_selection.date_to}")
        logging.info(f"Endpoints to extract: {params.data_selection.endpoints}")
        logging.info(f"Incremental mode: {params.destination.incremental}")

        return params

    def _initialize_api_client(self) -> DaktelaApiClient:
        """Initialize and return configured API client (authenticates during init)."""
        params = self._require_params()
        return DaktelaApiClient(
            url=params.connection.url,
            username=params.connection.username,
            password=params.connection.password,
            max_concurrent=params.destination.max_concurrent_requests,
            verify_ssl=params.connection.verify_ssl,
        )

    def _create_extractor(
        self,
        api_client: DaktelaApiClient,
    ) -> DaktelaExtractor:
        """Create and configure the extractor."""
        params = self._require_params()

        # Parse dates using keboola utils
        from_datetime = keboola.utils.get_past_date(params.data_selection.date_from).strftime("%Y-%m-%d %H:%M:%S")
        to_datetime = keboola.utils.get_past_date(params.data_selection.date_to).strftime("%Y-%m-%d %H:%M:%S")

        return DaktelaExtractor(
            api_client=api_client,
            table_configs={},
            component=self,
            url=params.connection.url,
            incremental=params.destination.incremental,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            requested_endpoints=params.data_selection.endpoints,
            batch_size=params.destination.batch_size,
        )

    def _apply_state(self) -> None:
        """Apply state for incremental processing."""
        params = self._require_params()
        if params.destination.incremental:
            state = self.get_state_file()
            last_timestamp = state.get("last_timestamp")

            if last_timestamp:
                logging.info(f"Incremental load: using last_timestamp={last_timestamp} as date_from")
                # Override date_from with last successful run timestamp
                params.data_selection.date_from = last_timestamp
            else:
                logging.info("Incremental load: no previous state found, performing full extraction")

    def _save_state(self) -> None:
        """Save state after successful extraction."""
        params = self._require_params()
        if params.destination.incremental:
            current_timestamp = datetime.now(timezone.utc).isoformat()
            state = {
                "last_timestamp": current_timestamp,
                "endpoints_extracted": params.data_selection.endpoints,
                "url": params.connection.url,
            }

            self.write_state_file(state)
            logging.info(f"Saved state: last_timestamp={current_timestamp}")

    def write_table_data(
        self,
        table_name: str,
        records: List[Dict[str, Any]],
        table_config: Dict[str, Any],
        incremental: bool,
        columns: List[str],
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
                primary_key=table_config.get("manifest_primary_key", ["id"]),
                incremental=incremental,
            )

            table_definitions[table_name] = out_table

            # Write header
            with open(out_table.full_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()

            logging.info(f"Created table definition for {table_name} with {len(columns)} columns")

        # Get table definition
        out_table = table_definitions.get(table_name)

        if not out_table:
            raise UserException(f"Table definition not found for {table_name}. This should not happen.")

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
            logging.warning(f"No table definition found for {table_name}, skipping manifest")

    def _get_table_definitions(self) -> Dict[str, Any]:
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
