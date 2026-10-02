"""Main extractor module for Daktela data extraction."""

import logging
from typing import TYPE_CHECKING, Any

from keboola.component.exceptions import UserException
from keboola.utils.header_normalizer import DefaultHeaderNormalizer

from configuration import DEFAULT_BATCH_SIZE
from daktela_client import DaktelaApiClient
from transformer import DataTransformer

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
        configured_fields: dict[str, list[str]] | None = None,
    ):
        """
        Initialize extractor.

        Args:
            api_client: Configured API client
            table_configs: Dictionary of table configurations
            component: Component instance for writing tables
            url: Base URL (e.g., https://customer.daktela.com)
            requested_endpoints: List of endpoint names to extract (typically one per job)
            batch_size: Number of records to process in each batch (default: 1000)
            date_from: Start date for filtering (for supported endpoints)
            date_to: End date for filtering (for supported endpoints)
            incremental: Whether to use incremental mode
            configured_fields: User-configured fields per endpoint (optional)
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
        self.configured_fields = configured_fields or {}
        self._table_columns: dict[str, list[str]] = {}
        # API fields actually requested per output table, used to prune stale
        # columns carried over from previous runs.
        self._requested_fields: dict[str, list[str] | None] = {}
        self._header_normalizer = DefaultHeaderNormalizer()

    async def extract_all(self):
        """
        Extract all requested endpoints asynchronously.

        Note: In row-based configuration mode, typically only one endpoint
        is requested per job execution (platform executes one row per job).
        """
        logging.info(
            f"Starting extraction for {len(self.requested_endpoints)} endpoint(s)"
        )

        if not self.requested_endpoints:
            raise UserException("No endpoints specified for extraction")

        # Extract each endpoint (typically just one per job)
        for endpoint in self.requested_endpoints:
            await self._extract_table(endpoint)

        # Persist the discovered output column set so the next run preserves
        # columns even when the source data turns sparse.
        if self._table_columns:
            self.component.save_output_columns(self._table_columns)

        logging.info("Extraction completed successfully")

    def _get_table_endpoint(self, table_name: str, table_config: dict[str, Any]) -> str:
        """Return endpoint override for table if configured."""
        return table_config.get("endpoint", table_name)

    def _get_fields_for_endpoint(self, table_name: str) -> list[str] | None:
        """
        Determine which fields to extract for an endpoint.

        Only user-configured fields are sent to the API. Schema state columns
        are NOT used because they contain post-transformation names (e.g.
        ``category_name`` instead of ``category``) that the API does not
        recognise.

        Args:
            table_name: Name of the endpoint/table

        Returns:
            List of field names or None to fetch all fields
        """
        if table_name in self.configured_fields:
            fields = self.configured_fields[table_name]
            if fields:
                fields = self._with_primary_key_fields(table_name, fields)
                logging.info(
                    f"Using user-configured fields for {table_name}: {len(fields)} fields"
                )
                return fields

        logging.info(
            f"No user-configured fields for {table_name}, will fetch all fields from API"
        )
        return None

    def _with_primary_key_fields(self, table_name: str, fields: list[str]) -> list[str]:
        """
        Append the primary-key columns to the requested API fields.

        Storage rejects an import whose primary key is not in the table header,
        so a ``fields`` list that omits the key makes the job fail with an
        error that does not name the real cause.  The key columns are requested
        in addition to what the user asked for; Daktela silently ignores a field
        name it does not recognise, so this cannot break an endpoint whose key
        is not a real API field.

        Args:
            table_name: Name of the endpoint/table
            fields: User-configured field names

        Returns:
            The field list, extended with any missing primary-key column
        """
        primary_keys = self.table_configs.get(table_name, {}).get("primary_keys") or []
        normalized = {self._normalize_field(f) for f in fields}

        extended = list(fields)
        for key in primary_keys:
            if self._normalize_field(key) not in normalized:
                extended.append(key)
                normalized.add(self._normalize_field(key))
                logging.info(
                    f"Added primary-key column '{key}' to the requested fields for "
                    f"{table_name}; Storage requires it in the output table."
                )
        return extended

    def _normalize_field(self, field: str) -> str:
        """Return the output column name an API field maps to after transformation."""
        flattened = "_".join(part for part in field.split(".") if part)
        return self._header_normalizer._normalize_column_name(flattened)

    def _prune_prior_columns(
        self,
        prior: list[str],
        fields: list[str] | None,
        keep: set[str],
    ) -> list[str]:
        """
        Drop prior-run columns that the current ``fields`` selection cannot produce.

        The saved column state exists so a sparse run does not lose columns that
        earlier runs wrote.  When the user narrows ``fields``, however, that same
        state keeps every previously seen column in the header forever, so the
        narrower selection appears to have no effect and personal data columns
        stay in the output table.  Pruning makes a ``fields`` change take effect
        on the next run, with no manual state reset.

        With no ``fields`` configured the state is returned unchanged, so the
        drift protection is unaffected for every other configuration.

        Args:
            prior: Column list persisted by previous runs
            fields: API fields requested for this run (None = all fields)
            keep: Columns to keep regardless (``id`` and the primary key)

        Returns:
            The prior columns that the current selection can still produce
        """
        if not fields:
            return prior

        allowed_exact = set(keep)
        allowed_prefixes = set()
        for field in fields:
            parts = [part for part in field.split(".") if part]
            if not parts:
                continue
            # "user.name" -> the flattened column "user_name"
            allowed_exact.add(self._normalize_field(field))
            # A relation that is null on a row stays a scalar column ("user"),
            # so the bare root is always a possible output column.
            root = self._header_normalizer._normalize_column_name(parts[0])
            allowed_exact.add(root)
            if len(parts) == 1:
                # The whole object was requested, so any of its children may appear.
                allowed_prefixes.add(f"{root}_")

        return [
            column
            for column in prior
            if column in allowed_exact
            or any(column.startswith(prefix) for prefix in allowed_prefixes)
        ]

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

        # Get fields to fetch using precedence logic
        fields = self._get_fields_for_endpoint(table_name)
        self._requested_fields[output_table_name] = fields

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
                    total_records += self._write_records(
                        output_table_name, table_config, write_batch
                    )
                    write_batch = []

            # Write remaining records from this page
            if write_batch:
                total_records += self._write_records(
                    output_table_name, table_config, write_batch
                )

        # Finalize table (write manifest)
        if total_records > 0:
            self.component.finalize_table(output_table_name)
            logging.info(
                f"Completed extraction for table: {table_name} ({total_records} records)"
            )
        else:
            logging.warning(f"No data found for table: {table_name}")

    def _get_columns_from_batch(self, records: list[dict[str, Any]]) -> list[str]:
        """
        Build the column list from ALL records in a batch.

        Scanning every record is necessary because relation fields (e.g.
        ``user``, ``contact``) may be ``null`` in some records and nested
        dicts in others.  After flattening, a null produces a scalar
        column ``user`` while a dict produces ``user_name``,
        ``user_title``, etc.  If only the first record is inspected and
        it happens to have a null, the flattened columns from later
        records are silently dropped.

        Args:
            records: All transformed records in the current batch

        Returns:
            Ordered list of column names (union of all records)
        """
        seen: dict[str, None] = {}
        for record in records:
            for key in record:
                if key not in seen:
                    seen[key] = None

        columns = ["id"]
        for key in seen:
            if key not in columns:
                columns.append(key)

        return columns

    def _resolve_primary_keys(
        self, output_table_name: str, table_config: dict[str, Any]
    ) -> None:
        """
        Check the primary key against the columns this run will write.

        Storage rejects the whole import when a primary-key column is not in the
        table, with "Primary key column name not found in schema", which does not
        say what to change.  A *default* key that the endpoint does not have is
        dropped with a warning, so the endpoint still extracts.  A key the user
        configured is their intent, so it raises an error that names the missing
        column instead.

        Args:
            output_table_name: Output table being written
            table_config: Table configuration (mutated in place)
        """
        primary_keys = table_config.get("primary_keys") or []
        if not primary_keys:
            return

        columns = set(self._table_columns.get(output_table_name, []))
        missing = [key for key in primary_keys if key not in columns]
        if not missing:
            return

        if table_config.get("primary_key_is_default"):
            remaining = [key for key in primary_keys if key not in missing]
            logging.warning(
                f"Default primary key column(s) {missing} are not present in "
                f"{output_table_name}; continuing without them. Set a primary key "
                f"in the row configuration if this table needs one."
            )
            table_config["primary_keys"] = remaining
            return

        raise UserException(
            f"Primary key column(s) {missing} are not present in the extracted data "
            f"for {output_table_name}. Available columns: {sorted(columns)}. "
            f"Change the primary key in the row configuration, or add the column to "
            f"the 'fields' list."
        )

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
            batch_columns = self._get_columns_from_batch(records)
            # Seed from prior-run state so columns discovered in past runs
            # remain in the output header even when today's data lacks them.
            # Without this, Storage rejects the import with "missing columns"
            # whenever the source data goes sparse for a flattened relation.
            prior = self.component.get_output_columns(output_table_name)
            # A narrowed `fields` selection must actually narrow the output, so
            # drop prior columns the current selection can no longer produce.
            kept = self._prune_prior_columns(
                prior,
                self._requested_fields.get(output_table_name),
                keep={"id", *(table_config.get("primary_keys") or [])},
            )
            if len(kept) != len(prior):
                logging.info(
                    f"Dropped {len(prior) - len(kept)} column(s) from the saved state "
                    f"for {output_table_name}: the configured 'fields' selection no "
                    f"longer produces them."
                )
            prior = kept
            self._table_columns[output_table_name] = list(
                dict.fromkeys(prior + batch_columns)
            )
            if prior:
                added = len(self._table_columns[output_table_name]) - len(prior)
                logging.info(
                    f"Seeded {output_table_name} with {len(prior)} column(s) "
                    f"from prior run; first batch added {added} new column(s)"
                )
            self._resolve_primary_keys(output_table_name, table_config)
        else:
            existing = set(self._table_columns[output_table_name])
            new_columns = list(
                dict.fromkeys(k for r in records for k in r if k not in existing)
            )
            if new_columns:
                extended = self._table_columns[output_table_name] + new_columns
                # Rewrite the CSV first; only commit the in-memory column list
                # once the (atomic) rewrite has succeeded, so an I/O failure
                # cannot leave the header and the tracked columns out of sync.
                self.component.rewrite_table_columns(output_table_name, extended)
                self._table_columns[output_table_name] = extended
                logging.info(
                    f"Extended column list for {output_table_name} with "
                    f"{len(new_columns)} new column(s)"
                )
                logging.debug(f"New columns for {output_table_name}: {new_columns}")

        self.component.write_table_data(
            table_name=output_table_name,
            records=records,
            table_config=table_config,
            columns=self._table_columns[output_table_name],
            incremental=self.incremental,
        )

        return len(records)
