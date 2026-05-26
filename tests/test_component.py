import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from keboola.component.exceptions import UserException  # noqa: E402
from configuration import Configuration  # noqa: E402
from extractor import DaktelaExtractor  # noqa: E402
from transformer import DataTransformer  # noqa: E402


class TestConfiguration(unittest.TestCase):
    """Test global configuration validation."""

    def test_valid_configuration(self):
        """Test valid global configuration parameters."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test_user",
                "#password": "test_password",
            }
        )

        self.assertEqual(config.connection.username, "test_user")
        self.assertEqual(config.connection.url, "https://demo.daktela.com")
        self.assertEqual(config.connection.verify_ssl, True)

    def test_url_storage(self):
        """Test URL is properly stored."""
        config = Configuration(
            connection={
                "url": "https://mycompany.daktela.com",
                "username": "test",
                "#password": "test",
            }
        )

        self.assertEqual(config.connection.url, "https://mycompany.daktela.com")

    def test_missing_url(self):
        """Test validation fails when URL is missing."""
        with self.assertRaises(UserException):
            Configuration.from_dict(
                {
                    "connection": {
                        "username": "test",
                        "#password": "test",
                        # Missing required 'url' field
                    }
                }
            )

    def test_default_values(self):
        """Test default values for optional fields."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test",
            }
        )

        self.assertEqual(config.advanced.batch_size, 1000)
        self.assertEqual(config.advanced.max_concurrent_requests, 10)
        self.assertEqual(config.debug, False)


class TestMergedConfiguration(unittest.TestCase):
    """Test merged configuration (root + row fields)."""

    def test_merged_configuration(self):
        """Test merged configuration with both root and row fields."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test",
            },
            endpoint="contacts",
            date_from="7 days ago",
            date_to="today",
        )

        # Root config fields
        self.assertEqual(config.connection.url, "https://demo.daktela.com")
        self.assertEqual(config.connection.username, "test")

        # Row config fields
        self.assertEqual(config.endpoint, "contacts")
        self.assertEqual(config.date_from, "7 days ago")
        self.assertEqual(config.date_to, "today")
        self.assertIsNone(config.fields)
        self.assertEqual(config.destination.incremental, False)
        self.assertIsNone(config.destination.primary_key)

    def test_merged_configuration_with_fields(self):
        """Test merged configuration with fields specified."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test",
            },
            endpoint="contacts",
            date_from="7 days ago",
            date_to="today",
            fields=["name", "email", "phone"],
        )

        self.assertEqual(config.endpoint, "contacts")
        self.assertIsNotNone(config.fields)
        self.assertEqual(len(config.fields), 3)
        self.assertIn("name", config.fields)

    def test_validate_for_extraction(self):
        """Test validation for extraction requires endpoint."""
        # Config without endpoint should fail validation
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test",
            }
        )

        with self.assertRaises(ValueError) as context:
            config.validate_for_extraction()

        self.assertIn("endpoint is required", str(context.exception))

    def test_row_fields_defaults(self):
        """Test row fields have proper defaults."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test",
            }
        )

        # Row fields should have defaults
        self.assertIsNone(config.endpoint)
        self.assertEqual(config.date_from, "7 days ago")
        self.assertEqual(config.date_to, "today")
        self.assertIsNone(config.fields)


class TestGetFieldsForEndpoint(unittest.TestCase):
    """Test that schema state columns are NOT sent to the API."""

    def _make_extractor(self, configured_fields=None):
        component = MagicMock()
        component.get_schema_for_endpoint.return_value = [
            "id",
            "name",
            "category_name",
            "user",
            "contact",
        ]
        return DaktelaExtractor(
            api_client=MagicMock(),
            table_configs={"tickets": {"primary_keys": ["name"]}},
            component=component,
            url="https://test.daktela.com",
            requested_endpoints=["tickets"],
            configured_fields=configured_fields,
        )

    def test_no_configured_fields_returns_none(self):
        """Without user-configured fields, should return None (fetch all)."""
        extractor = self._make_extractor()
        result = extractor._get_fields_for_endpoint("tickets")
        self.assertIsNone(result)

    def test_schema_state_not_used(self):
        """Schema state columns must NOT be returned for API field selection."""
        extractor = self._make_extractor()
        result = extractor._get_fields_for_endpoint("tickets")
        # Even though schema state has columns, they must not be used
        self.assertIsNone(result)
        # The method should not even call get_schema_for_endpoint
        extractor.component.get_schema_for_endpoint.assert_not_called()

    def test_user_configured_fields_used(self):
        """User-configured fields should be returned."""
        extractor = self._make_extractor(
            configured_fields={"tickets": ["name", "title", "user"]}
        )
        result = extractor._get_fields_for_endpoint("tickets")
        self.assertEqual(result, ["name", "title", "user"])


class TestGetColumnsFromBatch(unittest.TestCase):
    """Test that column list is built from ALL records, not just the first."""

    def _make_extractor(self):
        return DaktelaExtractor(
            api_client=MagicMock(),
            table_configs={"tickets": {"primary_keys": ["name"]}},
            component=MagicMock(),
            url="https://test.daktela.com",
            requested_endpoints=["tickets"],
        )

    def test_single_record(self):
        """Column list from a single record."""
        extractor = self._make_extractor()
        records = [{"id": "1", "name": "t1", "title": "Ticket 1"}]
        columns = extractor._get_columns_from_batch(records)
        self.assertEqual(columns, ["id", "name", "title"])

    def test_mixed_null_and_nested_fields(self):
        """Columns from records where a field is null in some and flattened in others."""
        extractor = self._make_extractor()
        # Simulates post-transformation records:
        # Record 1: user was null → kept as scalar "user"
        # Record 2: user was a dict → flattened to "user_name", "user_title"
        records = [
            {"id": "1", "name": "t1", "user": None},
            {"id": "2", "name": "t2", "user_name": "john", "user_title": "John Doe"},
        ]
        columns = extractor._get_columns_from_batch(records)
        self.assertIn("id", columns)
        self.assertIn("name", columns)
        self.assertIn("user", columns)
        self.assertIn("user_name", columns)
        self.assertIn("user_title", columns)

    def test_id_always_first(self):
        """The 'id' column should always be first."""
        extractor = self._make_extractor()
        records = [{"name": "t1", "id": "1", "title": "Ticket"}]
        columns = extractor._get_columns_from_batch(records)
        self.assertEqual(columns[0], "id")


class TestTransformerFlattenMixedFields(unittest.TestCase):
    """Test that the transformer handles mixed null/nested fields correctly."""

    def test_null_field_stays_scalar(self):
        """A null relation field produces a scalar column."""
        transformer = DataTransformer("tickets", {"primary_keys": ["name"]})
        records = [{"name": "t1", "user": None, "title": "Test"}]
        result = list(transformer.transform_records(records))
        self.assertEqual(len(result), 1)
        self.assertIn("user", result[0])
        self.assertIsNone(result[0]["user"])

    def test_nested_dict_field_flattened(self):
        """A nested dict field is flattened into sub-columns."""
        transformer = DataTransformer("tickets", {"primary_keys": ["name"]})
        records = [
            {
                "name": "t1",
                "user": {"name": "john", "title": "John Doe"},
                "title": "Test",
            }
        ]
        result = list(transformer.transform_records(records))
        self.assertEqual(len(result), 1)
        self.assertIn("user_name", result[0])
        self.assertEqual(result[0]["user_name"], "john")
        self.assertIn("user_title", result[0])
        self.assertEqual(result[0]["user_title"], "John Doe")
        # Original "user" key should NOT be in the flattened output
        self.assertNotIn("user", result[0])


if __name__ == "__main__":
    unittest.main()
