import sys
import unittest
from pathlib import Path

from keboola.component.exceptions import UserException
from configuration import Configuration

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


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
                "date_from": "-7",
                "date_to": "today",
                "tables": ["contacts", "activities"]
            }
        )

        self.assertEqual(config.connection.username, "test_user")
        self.assertEqual(config.connection.url, "https://demo.daktela.com")
        self.assertEqual(len(config.data_selection.tables), 2)

    def test_url_storage(self):
        """Test URL is properly stored."""
        config = Configuration(
            connection={
                "url": "https://mycompany.daktela.com",
                "username": "test",
                "#password": "test"
            },
            data_selection={
                "date_from": "-7",
                "date_to": "today",
                "tables": ["contacts"]
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
                    "date_from": "-7",
                    "date_to": "today",
                    "tables": ["contacts"]
                }
            )

    def test_date_validation(self):
        """Test date validation."""
        # Valid dates
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test"
            },
            data_selection={
                "date_from": "2024-01-01",
                "date_to": "2024-01-10",
                "tables": ["contacts"]
            }
        )
        self.assertIsNotNone(config.data_selection.date_from)
        self.assertIsNotNone(config.data_selection.date_to)

    def test_date_formats(self):
        """Test various date formats."""
        # Today
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test"
            },
            data_selection={
                "date_from": "-7",
                "date_to": "today",
                "tables": ["contacts"]
            }
        )
        self.assertIsNotNone(config.data_selection.date_from)
        self.assertIsNotNone(config.data_selection.date_to)

    def test_tables_list_parsing(self):
        """Test table list parsing."""
        config = Configuration(
            connection={
                "url": "https://demo.daktela.com",
                "username": "test",
                "#password": "test"
            },
            data_selection={
                "date_from": "-7",
                "date_to": "today",
                "tables": ["contacts", "activities", "tickets"]
            }
        )

        tables = config.data_selection.tables
        self.assertEqual(len(tables), 3)
        self.assertIn("contacts", tables)
        self.assertIn("activities", tables)
        self.assertIn("tickets", tables)


if __name__ == "__main__":
    unittest.main()
