from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pytoolkit import TDWSQLProvider

if __name__ == "__main__":
    """
    Usage: tdw_rdd_test.py <user> <passwd> <db_name> <table_name> [prr_parts] [sub_parts]
    """
    if len(sys.argv) < 5:
        print("Usage: tdw_rdd_test.py <user> <passwd> <db_name> <table_name> [prr_parts] [sub_parts]")
        exit(1)

    user_name = sys.argv[1].strip()
    password = sys.argv[2].strip()
    db = sys.argv[3].strip()
    tbl = sys.argv[4].strip()
    pri_parts = sys.argv[5].strip().split(",") if len(sys.argv) > 5 else None
    sub_parts = sys.argv[6].strip().split(",") if len(sys.argv) > 6 else None

    session = SparkSession \
        .builder \
        .appName("tdw_df_test") \
        .getOrCreate()
    tdw = TDWSQLProvider(session, user_name, password, db)
    df = tdw.table(tblName=tbl, priParts=pri_parts, subParts=sub_parts)
    df.printSchema()
    cnt = df.count()
    print('cnt: %s' % str(cnt))
