from __future__ import print_function

import sys

from pyspark import SparkContext
from pytoolkit import TDWProvider

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

    sc = SparkContext(appName="tdw-rdd-test")
    tdw = TDWProvider(sc, user_name, password, db)
    rdd = tdw.table(tblName=tbl, priParts=pri_parts, subParts=sub_parts, use_unicode=False)
    cnt = rdd.count()
    print('cnt: %s' % str(cnt))
    sc.stop()
