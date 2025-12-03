import sys
import unittest
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from keboola.component.exceptions import UserException  # noqa: E402
from configuration import Configuration  # noqa: E402


class TestConfiguration(unittest.TestCase):
    """Test configuration validation."""

    def test_valid_configuration(self):
        """Test valid configuration parameters."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test_user",
                "#password": "test_password"
            },
            data_selection={
                "date_from": "7 days ago",
                "date_to": "today",
                "endpoints": ["contacts", "activities"]
            }
        )

        self.assertEqual(config.connection.username, "test_user")
        self.assertEqual(config.connection.url, "https://demo.daktela.com")
        self.assertEqual(len(config.data_selection.endpoints), 2)

    def test_url_storage(self):
        """Test URL is properly stored."""
        config = Configuration(
            connection={
                "url": "https://mycompany.daktela.com",
                "username": "test",
                "#password": "test"
            },
            data_selection={
                "date_from": "7 days ago",
                "date_to": "today",
                "endpoints": ["contacts"]
            }
        )

        self.assertEqual(config.connection.url, "https://mycompany.daktela.com")

    def test_missing_url(self):
        """Test validation fails when URL is missing."""
        with self.assertRaises(UserException):
            Configuration(
                connection={
                    "username": "test",
                    "#password": "test"
                    # Missing required 'url' field
                },
                data_selection={
                    "date_from": "7 days ago",
                    "date_to": "today",
                    "endpoints": ["contacts"]
                }
            )

    def test_endpoints_list_parsing(self):
        """Test endpoints list parsing."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test"
            },
            data_selection={
                "date_from": "7 days ago",
                "date_to": "today",
                "endpoints": ["contacts", "activities", "tickets"]
            }
        )

        endpoints = config.data_selection.endpoints
        self.assertEqual(len(endpoints), 3)
        self.assertIn("contacts", endpoints)
        self.assertIn("activities", endpoints)
        self.assertIn("tickets", endpoints)


if __name__ == "__main__":
    unittest.main()
