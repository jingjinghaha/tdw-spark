from __future__ import print_function

import sys

from pyspark import SparkContext
from pytoolkit import TableDesc
from pytoolkit import TDWUtil

if __name__ == "__main__":
    """
    Usage: tdw_ddl_test.py <user> <passwd> <db_name>
    """
    if len(sys.argv) < 4:
        print("Usage: tdw_rdd_test.py <user> <passwd> <db_name>")
        exit(1)

    user_name = sys.argv[1].strip()
    password = sys.argv[2].strip()
    db = sys.argv[3].strip()

    sc = SparkContext(appName="tdw-ddl-test")
    tdw = TDWUtil(user_name, password, db)
    # create table
    table_desc = TableDesc().setTblName("tdw_ddl_test"). \
        setCols([['col1', 'bigint', 'this is col1'],
                 ['col2', 'string','this is col2'],
                 ['col3', 'string', "this is col3"]]). \
        setComment("this is tdw-ddl-test"). \
        setCompress(True). \
        setFileFormat("rcfile"). \
        setPartType("range"). \
        setPartField("col1"). \
        setSubPartType("list"). \
        setSubPartField("col2")

    tdw.createTable(table_desc)
    # add range partition
    tdw.createRangePartition("tdw_ddl_test", "p1", "20")
    tdw.createRangePartition("tdw_ddl_test", "p2", "40")
    # add list partition
    tdw.createListPartition("tdw_ddl_test", "sp1", "haha", level=1)
    tdw.createListPartition("tdw_ddl_test", "sp2", "qq,wx", level=1)
    # drop partition
    tdw.dropPartition("tdw_ddl_test", "p2")
    tdw.dropPartition("tdw_ddl_test", "sp1", level=1)
    # get table meta info
    tblInfo = tdw.getTableInfo("tdw_ddl_test")
    print ("colNames: %s" % tblInfo.colNames)
    print ("colTypes: %s" % tblInfo.colTypes)
    print ("partitions: %s" % tblInfo.partitions)
    tdw.dropTable("tdw_ddl_test")
    sc.stop()
