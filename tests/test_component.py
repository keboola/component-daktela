import sys
import unittest
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from keboola.component.exceptions import UserException  # noqa: E402
from configuration import Configuration  # noqa: E402


class TestConfiguration(unittest.TestCase):
    """Test global configuration validation."""

    def test_valid_configuration(self):
        """Test valid global configuration parameters."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test_user",
                "#password": "test_password"
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
                "#password": "test"
            }
        )

        self.assertEqual(config.connection.url, "https://mycompany.daktela.com")

    def test_missing_url(self):
        """Test validation fails when URL is missing."""
        with self.assertRaises(UserException):
            Configuration.from_dict({
                "connection": {
                    "username": "test",
                    "#password": "test"
                    # Missing required 'url' field
                }
            })

    def test_default_values(self):
        """Test default values for optional fields."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test"
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
                "#password": "test"
            },
            endpoint="contacts",
            date_from="7 days ago",
            date_to="today"
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
                "#password": "test"
            },
            endpoint="contacts",
            date_from="7 days ago",
            date_to="today",
            fields=["name", "email", "phone"]
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
                "#password": "test"
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
                "#password": "test"
            }
        )

        # Row fields should have defaults
        self.assertIsNone(config.endpoint)
        self.assertEqual(config.date_from, "7 days ago")
        self.assertEqual(config.date_to, "today")
        self.assertIsNone(config.fields)


if __name__ == "__main__":
    unittest.main()
