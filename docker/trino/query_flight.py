"""Run a SQL query through the Trino Arrow Flight catalog."""

import argparse
import os

from trino.dbapi import connect


def main():
    """Connect to Trino and stream the selected SQL query's result rows."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        default="SELECT count(*) FROM flight.test_schema.test_table",
        help="SQL statement to execute",
    )
    args = parser.parse_args()
    connection = connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "18080")),
        user=os.getenv("TRINO_USER", "developer"),
        catalog="flight",
    )
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(args.execute.strip().rstrip(";"))
            for row in cursor:
                print(row)
        finally:
            cursor.close()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
