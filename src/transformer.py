"""
Data transformation module for Daktela extractor.

This module transforms raw API responses into CSV-ready format through a series of steps:
1. Flatten nested JSON structures (up to 2 levels)
2. Clean HTML tags from string values
3. Handle list columns and list-of-dicts columns
4. Sanitize column names for Keboola compatibility
5. Add required output columns (id)

The transformation pipeline ensures data consistency and compatibility with
Keboola Storage tables.
"""

import re
import logging
from typing import Any
from keboola.utils.header_normalizer import DefaultHeaderNormalizer


class DataTransformer:
    """Transforms raw API data into structured CSV-ready format."""

    def __init__(self, table_name: str, table_config: dict[str, Any]):
        """
        Initialize data transformer.

        Args:
            table_name: Name of the table
            table_config: Configuration dict for the table being transformed
        """
        self.table_name = table_name
        self.primary_keys = table_config.get("primary_keys", [])
        self.secondary_keys = table_config.get("secondary_keys", [])
        self.list_columns = table_config.get("list_columns", [])
        self.list_of_dicts_columns = table_config.get("list_of_dicts_columns", [])
        self.header_normalizer = DefaultHeaderNormalizer()

    def transform_records(self, records: list[dict[str, Any]]):
        """
        Transform list of API records into CSV-ready format (generator).

        This method applies a transformation pipeline with the following steps:
        1. Flatten nested JSON structures
        2. Clean HTML from string values
        3. Handle list/list-of-dicts columns (may create multiple rows)
        4. Sanitize column names for Keboola compatibility
        5. Add required output columns (id)

        Args:
            records: List of raw API records

        Yields:
            Transformed records ready for CSV output
        """
        input_count = len(records)
        output_count = 0

        for record in records:
            # Step 1: Flatten nested JSON (up to 2 levels deep)
            flattened = self._flatten_json(record)

            # Step 2: Clean HTML tags from string values
            cleaned = self._clean_html(flattened)

            # Step 3: Handle list columns and list-of-dicts columns
            # Note: This may explode one record into multiple rows
            rows = self._handle_lists(cleaned)

            for row in rows:
                # Step 4: Sanitize column names for Keboola Storage compatibility
                sanitized = self._sanitize_columns(row)

                # Step 5: Add required output columns (id)
                final_row = self._add_output_columns(sanitized)

                output_count += 1
                yield final_row

        logging.info(f"Transformed {input_count} records into {output_count} rows for table {self.table_name}")

    def _flatten_json(self, data: dict[str, Any], parent_key: str = "", level: int = 0) -> dict[str, Any]:
        """
        Flatten nested JSON dictionaries up to 2 levels.

        Converts dot notation to underscores (e.g., user.name -> user_name).

        Args:
            data: Dictionary to flatten
            parent_key: Parent key for nested items
            level: Current nesting level

        Returns:
            Flattened dictionary
        """
        items = {}

        for key, value in data.items():
            new_key = f"{parent_key}_{key}" if parent_key else key

            if isinstance(value, dict) and level < 2:
                # Recurse into nested dict (up to 2 levels)
                items.update(self._flatten_json(value, new_key, level + 1))
            else:
                items[new_key] = value

        return items

    def _clean_html(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Remove HTML tags from string values.

        Args:
            data: Dictionary with potentially HTML-containing strings

        Returns:
            Dictionary with cleaned strings
        """
        cleaned = {}
        html_pattern = re.compile(r"<.*?>")

        for key, value in data.items():
            if isinstance(value, str):
                # Remove HTML tags
                cleaned_value = html_pattern.sub("", value)
                # Convert empty strings and whitespace to None
                if not cleaned_value or cleaned_value.isspace():
                    cleaned[key] = None
                else:
                    cleaned[key] = cleaned_value
            else:
                cleaned[key] = value

        return cleaned

    def _handle_lists(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Handle list columns and list of dicts columns.

        - list_columns: Explode lists into multiple rows
        - list_of_dicts_columns: Split into multiple rows and flatten dict keys

        Args:
            data: Dictionary with potential list values

        Returns:
            List of dictionaries (may be multiple rows from a single input)
        """
        rows = [data]

        # Handle list_columns first
        for list_col in self.list_columns:
            if list_col not in data:
                continue

            value = data[list_col]
            if not isinstance(value, list):
                continue

            # Explode list into multiple rows
            new_rows = []
            for row in rows:
                if not value:  # Empty list
                    new_rows.append(row)
                else:
                    for item in value:
                        new_row = row.copy()
                        new_row[list_col] = item
                        new_rows.append(new_row)
            rows = new_rows

        # Handle list_of_dicts_columns
        for list_dict_col in self.list_of_dicts_columns:
            if list_dict_col not in data:
                continue

            value = data[list_dict_col]
            if not isinstance(value, list):
                continue

            # Explode list and flatten dicts
            new_rows = []
            for row in rows:
                if not value:  # Empty list
                    # Remove the list column
                    new_row = {k: v for k, v in row.items() if k != list_dict_col}
                    new_rows.append(new_row)
                else:
                    for item in value:
                        new_row = {k: v for k, v in row.items() if k != list_dict_col}
                        if isinstance(item, dict):
                            # Flatten dict keys as new columns
                            for dict_key, dict_value in item.items():
                                new_row[f"{list_dict_col}_{dict_key}"] = dict_value
                        new_rows.append(new_row)
            rows = new_rows

        return rows

    def _sanitize_columns(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Sanitize column names using keboola.utils.header_normalizer.

        Args:
            data: Dictionary with unsanitized column names

        Returns:
            Dictionary with sanitized column names
        """
        sanitized = {}
        for key, value in data.items():
            # Use header_normalizer to clean column names
            clean_key = self.header_normalizer._normalize_column_name(key)
            sanitized[clean_key] = value

        return sanitized

    def _add_output_columns(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Add required output columns: id.

        Args:
            data: Dictionary with data columns

        Returns:
            Dictionary with id column added
        """
        output = {}
        key_columns = self.primary_keys + self.secondary_keys

        id_parts = []
        for key in key_columns:
            value = data.get(key)
            if value is not None:
                id_parts.append(str(value))

        output["id"] = "_".join(id_parts) if id_parts else ""

        # Add all other data columns
        for key, value in data.items():
            output[key] = value

        return output
