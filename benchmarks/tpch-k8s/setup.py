#!/usr/bin/env python3
"""Creates the two TPC-H SF100 schemas in StarRocks: `direct` (views over Parquet on MinIO via FILES()) and the
`flight_c` catalog (Flight SQL cluster, schema sf100). Usage: setup.py FE_HOST FE_PORT MINIO_USER MINIO_PASSWORD"""
import sys

import pymysql

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def main() -> int:
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    cur = pymysql.connect(host=host, port=port, user="root", autocommit=True).cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS direct")
    for t in TABLES:
        cur.execute(
            'CREATE OR REPLACE VIEW direct.%s AS SELECT * FROM FILES("path"="s3://tpch/sf100/%s/*.parquet",'
            '"format"="parquet","aws.s3.endpoint"="http://minio:9000","aws.s3.access_key"="%s",'
            '"aws.s3.secret_key"="%s","aws.s3.use_instance_profile"="false","aws.s3.enable_path_style_access"="true",'
            '"aws.s3.region"="us-east-1")' % (t, t, user, password))
    cur.execute("DROP CATALOG IF EXISTS flight_c")
    cur.execute('CREATE EXTERNAL CATALOG flight_c PROPERTIES("type"="flight",'
                '"flight.uri"="grpc://flight-0.flight:32010")')
    cur.execute("SHOW TABLES FROM flight_c.sf100")
    print("flight tables:", [r[0] for r in cur.fetchall()])
    cur.execute("DESC flight_c.sf100.lineitem")
    print("lineitem via flight:", [(r[0], r[1]) for r in cur.fetchall()])
    cur.execute("DESC direct.lineitem")
    print("lineitem direct:", [(r[0], r[1]) for r in cur.fetchall()])
    return 0


if __name__ == "__main__":
    sys.exit(main())
