import sys
import unittest
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from keboola.component.exceptions import UserException  # noqa: E402
from configuration import Configuration, RowConfiguration  # noqa: E402


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
        self.assertEqual(config.advanced.max_concurrent_endpoints, 3)
        self.assertEqual(config.debug, False)


class TestRowConfiguration(unittest.TestCase):
    """Test row configuration validation."""

    def test_valid_row_configuration(self):
        """Test valid row configuration."""
        row_config = RowConfiguration(
            endpoint="contacts",
            date_from="7 days ago",
            date_to="today"
        )

        self.assertEqual(row_config.endpoint, "contacts")
        self.assertEqual(row_config.date_from, "7 days ago")
        self.assertEqual(row_config.date_to, "today")
        self.assertIsNone(row_config.fields)
        self.assertEqual(row_config.destination.incremental, False)
        self.assertIsNone(row_config.destination.primary_key)

    def test_row_configuration_with_fields(self):
        """Test row configuration with fields specified."""
        row_config = RowConfiguration(
            endpoint="contacts",
            date_from="7 days ago",
            date_to="today",
            fields=["name", "email", "phone"]
        )

        self.assertEqual(row_config.endpoint, "contacts")
        self.assertIsNotNone(row_config.fields)
        self.assertEqual(len(row_config.fields), 3)
        self.assertIn("name", row_config.fields)

    def test_missing_required_field(self):
        """Test validation fails when required field is missing."""
        with self.assertRaises(UserException):
            RowConfiguration.from_dict({
                "date_from": "7 days ago",
                "date_to": "today"
                # Missing required 'endpoint' field
            })

    def test_multiple_row_configs(self):
        """Test creating multiple row configurations."""
        row1 = RowConfiguration(
            endpoint="contacts",
            date_from="7 days ago",
            date_to="today"
        )
        row2 = RowConfiguration(
            endpoint="activities",
            date_from="3 days ago",
            date_to="today",
            fields=["time", "user", "title"]
        )
        row3 = RowConfiguration(
            endpoint="tickets",
            date_from="7 days ago",
            date_to="today"
        )

        rows = [row1, row2, row3]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].endpoint, "contacts")
        self.assertEqual(rows[1].endpoint, "activities")
        self.assertIsNotNone(rows[1].fields)
        self.assertEqual(rows[2].endpoint, "tickets")


if __name__ == "__main__":
    unittest.main()
