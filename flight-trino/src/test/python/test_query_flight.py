"""Unit tests for the Python Trino query entry point."""

import contextlib
import importlib.util
import io
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch


SCRIPT = Path(__file__).resolve().parents[4] / "docker/trino/query_flight.py"
SPEC = importlib.util.spec_from_file_location("query_flight", SCRIPT)
QUERY_FLIGHT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(QUERY_FLIGHT)


class QueryFlightTest(unittest.TestCase):
    """Verify connection settings, result streaming and resource cleanup."""

    def test_uses_environment_and_supplied_sql(self):
        """Pass the endpoint and SQL through to the Trino client."""
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.__iter__.return_value = iter([(1000,)])
        with patch.dict(os.environ, {"TRINO_HOST": "trino", "TRINO_PORT": "8080",
                                     "TRINO_USER": "alice"}), \
                patch("sys.argv", ["query_flight.py", "--execute", "SELECT count(*) FROM t;"]), \
                patch.object(QUERY_FLIGHT, "connect", return_value=connection) as connect, \
                contextlib.redirect_stdout(io.StringIO()) as output:
            QUERY_FLIGHT.main()
        connect.assert_called_once_with(host="trino", port=8080, user="alice", catalog="flight")
        cursor.execute.assert_called_once_with("SELECT count(*) FROM t")
        self.assertEqual("(1000,)\n", output.getvalue())
        cursor.close.assert_called_once()
        connection.close.assert_called_once()

    def test_closes_resources_when_execution_fails(self):
        """Release the cursor and connection after a SQL error."""
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.execute.side_effect = RuntimeError("query failed")
        with patch("sys.argv", ["query_flight.py"]), \
                patch.object(QUERY_FLIGHT, "connect", return_value=connection), \
                self.assertRaisesRegex(RuntimeError, "query failed"):
            QUERY_FLIGHT.main()
        cursor.close.assert_called_once()
        connection.close.assert_called_once()

    def test_closes_connection_when_cursor_creation_fails(self):
        """Release the connection even when no cursor can be created."""
        connection = MagicMock()
        connection.cursor.side_effect = RuntimeError("cursor failed")
        with patch("sys.argv", ["query_flight.py"]), \
                patch.object(QUERY_FLIGHT, "connect", return_value=connection), \
                self.assertRaisesRegex(RuntimeError, "cursor failed"):
            QUERY_FLIGHT.main()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
