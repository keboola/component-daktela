"""
Async HTTP client for Daktela API with authentication, retry logic and pagination.
"""

import asyncio
import logging
import warnings
import requests
from typing import Dict, List, Any, Optional

from keboola.http_client import AsyncHttpClient
from keboola.component.exceptions import UserException

from configuration import DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_BATCH_SIZE

# API Client constants
DEFAULT_MAX_RETRIES = 8
"""Maximum number of retry attempts for failed API requests."""

DEFAULT_PAGE_LIMIT = 1000
"""Default number of records to fetch per API request."""

LINEAR_BACKOFF_SECONDS = 1
"""Base multiplier for linear backoff retry delay (delay = attempt * multiplier)."""

AUTH_TIMEOUT_SECONDS = 30
"""Timeout for authentication requests."""


class DaktelaApiClient:
    """Async HTTP client for Daktela API with built-in authentication, retry and pagination."""

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        max_retries: int = DEFAULT_MAX_RETRIES,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
        verify_ssl: bool = True,
    ):
        """
        Initialize API client and authenticate.

        Args:
            url: Base URL for Daktela API
            username: Daktela account username
            password: Daktela account password
            max_retries: Maximum number of retry attempts
            max_concurrent: Maximum concurrent requests
            verify_ssl: Whether to verify SSL certificates (default: True)
        """
        self.url = url
        self.username = username
        self.password = password
        self.max_retries = max_retries
        self.max_concurrent = max_concurrent
        self.verify_ssl = verify_ssl
        self.client = None  # Will be initialized in __aenter__
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Authenticate synchronously during initialization
        self.access_token = self._authenticate()

    def _authenticate(self) -> str:
        """
        Authenticate with Daktela API and retrieve access token.

        Returns:
            str: Access token for subsequent API requests

        Raises:
            UserException: If authentication fails or connection error occurs
        """
        login_url = f"{self.url}/api/v6/login.json"
        params = {"username": self.username, "password": self.password, "only_token": 1}

        try:
            logging.info(f"Attempting to authenticate with Daktela API at {self.url}")
            if not self.verify_ssl:
                warnings.filterwarnings("ignore", message="Unverified HTTPS request")
                logging.warning("SSL verification is disabled for authentication. This is insecure.")

            response = requests.post(
                login_url,
                params=params,
                verify=self.verify_ssl,
                timeout=AUTH_TIMEOUT_SECONDS,
            )

            # Check for successful response
            if response.status_code != 200:
                raise UserException(
                    f"Invalid response from Daktela API. Status code: {response.status_code}. "
                    f"Response: {response.text[:200]}"
                )

            # Parse response
            try:
                result = response.json()
            except Exception as e:
                raise UserException(f"Failed to parse authentication response: {str(e)}")

            # Extract access token
            if "result" not in result or not result["result"]:
                raise UserException(f"Invalid token in authentication response. Response: {response.text[:200]}")

            access_token = result["result"]
            logging.info("Successfully authenticated with Daktela API")

            return access_token

        except requests.exceptions.ConnectionError as e:
            raise UserException(f"Server not responding. Failed to connect to {self.url}: {str(e)}")
        except requests.exceptions.Timeout as e:
            raise UserException(f"Connection timeout when connecting to {self.url}: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise UserException(f"Request failed: {str(e)}")

    async def __aenter__(self):
        """Async context manager entry."""
        self.client = AsyncHttpClient(self.url, verify_ssl=self.verify_ssl)
        await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)

    async def fetch_table_data_batched(
        self,
        table_name: str,
        fields: List[str],
        filters: Dict[str, Any],
        limit: int = DEFAULT_PAGE_LIMIT,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """
        Fetch data for a table in batches (generator for memory efficiency).

        Args:
            table_name: Name of the table to fetch
            fields: List of fields to include
            filters: Dictionary of filters to apply
            limit: Number of records per page
            batch_size: Number of records to accumulate before yielding

        Yields:
            Batches of records from the API
        """
        # Build endpoint (relative to base URL)
        endpoint = f"api/v6/{table_name}.json"

        # Build query parameters
        params = {"accessToken": self.access_token}

        # Add fields if specified
        if fields:
            params["fields"] = ",".join(fields)

        # Add filters
        params.update(filters)

        # First, get total count
        params_count = params.copy()
        params_count["skip"] = 0
        params_count["take"] = 1

        logging.info(f"Fetching total count for table: {table_name}")
        first_response = await self._request_with_retry(endpoint, params_count)

        if not first_response or "result" not in first_response:
            logging.warning(f"No data found for table: {table_name}")
            return

        total = first_response["result"].get("total", 0)
        logging.info(f"Table {table_name}: Total entries: {total}, Batches: {(total + limit - 1) // limit}")

        if total == 0:
            return

        # Fetch pages in batches to manage memory
        batch = []
        for offset in range(0, total, limit):
            params_page = params.copy()
            params_page["skip"] = offset
            params_page["take"] = limit

            records = await self._fetch_page_direct(endpoint, params_page, table_name, offset)
            batch.extend(records)

            # Yield batch when it reaches batch_size
            if len(batch) >= batch_size:
                logging.debug(f"Yielding batch of {len(batch)} records")
                yield batch
                batch = []

        # Yield remaining records
        if batch:
            logging.debug(f"Yielding final batch of {len(batch)} records")
            yield batch

    async def fetch_table_data(
        self,
        table_name: str,
        fields: List[str],
        filters: Dict[str, Any],
        limit: int = DEFAULT_PAGE_LIMIT,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all data for a table with pagination (loads all into memory).

        Note: For large datasets, consider using fetch_table_data_batched() instead.

        Args:
            table_name: Name of the table to fetch
            fields: List of fields to include
            filters: Dictionary of filters to apply
            limit: Number of records per page

        Returns:
            List of records from the API
        """
        # Build endpoint (relative to base URL)
        endpoint = f"api/v6/{table_name}.json"

        # Build query parameters
        params = {"accessToken": self.access_token}

        # Add fields if specified
        if fields:
            params["fields"] = ",".join(fields)

        # Add filters
        params.update(filters)

        # First, get total count
        params_count = params.copy()
        params_count["skip"] = 0
        params_count["take"] = 1

        logging.info(f"Fetching total count for table: {table_name}")
        first_response = await self._request_with_retry(endpoint, params_count)

        if not first_response or "result" not in first_response:
            logging.warning(f"No data found for table: {table_name}")
            return []

        total = first_response["result"].get("total", 0)
        logging.info(f"Table {table_name}: Total entries: {total}, Batches: {(total + limit - 1) // limit}")

        if total == 0:
            return []

        # Fetch all pages
        all_records = []
        tasks = []

        for offset in range(0, total, limit):
            params_page = params.copy()
            params_page["skip"] = offset
            params_page["take"] = limit
            tasks.append(self._fetch_page(endpoint, params_page, table_name, offset))

        # Execute all requests concurrently
        results = await asyncio.gather(*tasks)

        # Combine all results
        for records in results:
            if records:
                all_records.extend(records)

        logging.info(f"Table {table_name}: Fetched {len(all_records)} records")
        return all_records

    async def fetch_dependent_table_data(
        self,
        parent_table: str,
        parent_id: str,
        child_table: str,
        fields: List[str],
        filters: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Fetch data for a dependent table (child of parent).

        Args:
            parent_table: Name of parent table
            parent_id: ID of parent record
            child_table: Name of child table
            fields: List of fields to include
            filters: Dictionary of filters to apply

        Returns:
            List of records from the API
        """
        # Build endpoint for dependent table
        endpoint = f"api/v6/{parent_table}/{parent_id}/{child_table}.json"

        # Build query parameters
        params = {"accessToken": self.access_token}

        # Add fields if specified
        if fields:
            params["fields"] = ",".join(fields)

        # Add filters
        params.update(filters)

        logging.debug(f"Fetching dependent table: {child_table} for parent {parent_table}:{parent_id}")
        response = await self._request_with_retry(endpoint, params)

        if not response or "result" not in response:
            return []

        data = response["result"].get("data", [])
        return data if isinstance(data, list) else []

    async def _fetch_page(
        self, endpoint: str, params: Dict[str, Any], table_name: str, offset: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of data with concurrency limiting.

        Args:
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            table_name: Name of table (for logging)
            offset: Offset for this page

        Returns:
            List of records from this page
        """
        async with self.semaphore:
            return await self._fetch_page_direct(endpoint, params, table_name, offset)

    async def _fetch_page_direct(
        self, endpoint: str, params: Dict[str, Any], table_name: str, offset: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single page of data without concurrency limiting.

        Args:
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            table_name: Name of table (for logging)
            offset: Offset for this page

        Returns:
            List of records from this page
        """
        logging.debug(f"Fetching {table_name} page at offset {offset}")
        response = await self._request_with_retry(endpoint, params)

        if not response or "result" not in response:
            return []

        data = response["result"].get("data", [])
        return data if isinstance(data, list) else []

    async def _request_with_retry(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request with linear backoff retry logic.

        Args:
            endpoint: API endpoint (relative to base URL)
            params: Query parameters

        Returns:
            Response JSON as dictionary

        Raises:
            UserException: After all retries exhausted
        """
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                # AsyncHttpClient.get() returns parsed JSON directly
                # It raises exceptions for non-200 status codes
                data = await self.client.get(endpoint, params=params)
                return data

            except asyncio.TimeoutError as e:
                logging.warning(f"Request timeout. Attempt {attempt + 1}/{self.max_retries}")
                last_exception = e

            except Exception as e:
                logging.warning(f"Request failed: {str(e)}. Attempt {attempt + 1}/{self.max_retries}")
                last_exception = e

            # Linear backoff: 1s, 2s, 3s, ..., max_retries seconds
            if attempt < self.max_retries - 1:
                wait_time = (attempt + 1) * LINEAR_BACKOFF_SECONDS
                logging.debug(f"Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)

        # All retries exhausted
        error_msg = f"Failed to fetch data after {self.max_retries} attempts"
        if last_exception:
            error_msg += f": {str(last_exception)}"
        raise UserException(error_msg)
