"""
Async HTTP client for Daktela API with authentication and pagination.
Retry logic is handled by the Keboola AsyncHttpClient.
"""

import asyncio
import json
import logging
import warnings
import requests
from typing import Dict, List, Any, Optional

from keboola.http_client import AsyncHttpClient
from keboola.component.exceptions import UserException

from configuration import DEFAULT_MAX_CONCURRENT_REQUESTS, DEFAULT_BATCH_SIZE

# API Client constants
DEFAULT_PAGE_LIMIT = 1000
"""Default number of records to fetch per API request."""

AUTH_TIMEOUT_SECONDS = 30
"""Timeout for authentication requests."""

# Endpoints that support date filtering via filter[field]=edited
FILTER_PAGINATED_ENDPOINTS = {"tickets", "contacts", "activities"}
"""Endpoints that support filtering on the 'edited' field."""


class DaktelaApiClient:
    """Async HTTP client for Daktela API with built-in authentication and pagination."""

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
        verify_ssl: bool = True,
    ):
        """
        Initialize API client and authenticate.

        Args:
            url: Base URL for Daktela API
            username: Daktela account username
            password: Daktela account password
            max_concurrent: Maximum concurrent requests
            verify_ssl: Whether to verify SSL certificates (default: True)
        """
        self.url = url
        self.username = username
        self.password = password
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

            # Extract just the accessToken string from the result object
            token_data = result["result"]
            if isinstance(token_data, dict) and "accessToken" in token_data:
                access_token = token_data["accessToken"]
            else:
                # Fallback for older API versions that might return token directly
                access_token = token_data

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

    def _prepare_endpoint(self, endpoint: str) -> str:
        """Ensure endpoint includes api prefix and .json suffix."""
        cleaned = endpoint.lstrip("/")
        if not cleaned.endswith(".json"):
            cleaned = f"{cleaned}.json"
        if not cleaned.startswith("api/"):
            cleaned = f"api/v6/{cleaned}"
        return cleaned

    async def fetch_table_data_batched(
        self,
        table_name: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: int = DEFAULT_PAGE_LIMIT,
        batch_size: int = DEFAULT_BATCH_SIZE,
        endpoint: Optional[str] = None,
    ):
        """
        Fetch data for a table in batches (generator for memory efficiency).

        For endpoints that support filtering (tickets, contacts, activities),
        applies date range filter on 'edited' field.

        Filtering modes:
        - One date only: Simple format filter[field]=edited&filter[operator]=gte&filter[value]=date
        - Both dates: Complex JSON format {"logic":"and","filters":[...]}

        Complex filter example structure:
        {
            "filter": {
                "logic": "or",
                "filters": [
                    {"field": "firstname", "operator": "eq", "value": "John"},
                    {"field": "firstname", "operator": "eq", "value": "James"},
                    {
                        "logic": "and",
                        "filters": [
                            {"field": "firstname", "operator": "eq", "value": "David"},
                            {"field": "lastname", "operator": "eq", "value": "Smith"}
                        ]
                    }
                ]
            }
        }

        Args:
            table_name: Name of the table to fetch
            date_from: Start date (edited >= date_from)
            date_to: End date (edited <= date_to)
            limit: Number of records per page
            batch_size: Number of records to accumulate before yielding

        Yields:
            Batches of records from the API
        """
        endpoint_path = self._prepare_endpoint(endpoint or table_name)
        params = {"accessToken": self.access_token}

        # Apply date filtering for supported endpoints
        if table_name in FILTER_PAGINATED_ENDPOINTS:
            filters = []

            if date_from:
                filters.append({"field": "edited", "operator": "gte", "value": date_from})

            if date_to:
                filters.append({"field": "edited", "operator": "lte", "value": date_to})

            if len(filters) == 2:
                # Both dates: use AND logic (JSON complex filter format)
                logging.info(f"Date filter for {table_name}: {date_from} to {date_to}")
                params["filter"] = json.dumps({"logic": "and", "filters": filters})
            elif len(filters) == 1:
                # Single date: use simple format
                f = filters[0]
                logging.info(f"Date filter for {table_name}: edited {f['operator']} {f['value']}")
                params["filter[field]"] = f["field"]
                params["filter[operator]"] = f["operator"]
                params["filter[value]"] = f["value"]

        # First, get total count
        params_count = params.copy()
        params_count["skip"] = 0
        params_count["take"] = 1

        logging.info(f"Fetching total count for table: {table_name}")
        first_response = await self.client.get(endpoint_path, params=params_count)

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

            records = await self._fetch_page_direct(endpoint_path, params_page, table_name, offset)
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
        filters: Dict[str, Any],
        limit: int = DEFAULT_PAGE_LIMIT,
        endpoint: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all data for a table with pagination (loads all into memory).

        Note: For large datasets, consider using fetch_table_data_batched() instead.

        Args:
            table_name: Name of the table to fetch
            filters: Dictionary of filters to apply
            limit: Number of records per page

        Returns:
            List of records from the API
        """
        # Build endpoint (relative to base URL)
        endpoint = self._prepare_endpoint(endpoint or table_name)

        # Build query parameters
        params = {"accessToken": self.access_token}

        # Add filters
        params.update(filters)

        # First, get total count
        params_count = params.copy()
        params_count["skip"] = 0
        params_count["take"] = 1

        logging.info(f"Fetching total count for table: {table_name}")
        first_response = await self.client.get(endpoint, params=params_count)

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
        response = await self.client.get(endpoint, params=params)

        if not response or "result" not in response:
            return []

        data = response["result"].get("data", [])
        return data if isinstance(data, list) else []
