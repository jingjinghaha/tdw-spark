package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWFunctions._
import com.tencent.tdw.spark.toolkit.tdw.TDWProvider
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sharkdtu
 * 2015/9/1
 */

object TDWRDDTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val cmdArgs = new GeneralArgParser(args)

    val srcTbl = cmdArgs.getStringValue("src_tbl")
    val priParts = cmdArgs.getCommaSplitArrayValue("pri_parts")
    val subParts = cmdArgs.getCommaSplitArrayValue("sub_parts")

    val tdwurl = cmdArgs.getStringValue("tdwurl")

    val tdw = new TDWProvider(sc, "test_user", "test_passwd", "test_db")
    //val tdw1 = new TDWProvider(sc, "aa")
    // read from none partition table
//    val data = tdw.table(tblName)
    // read from pri partition table
//    val data = tdw.table(tblName, priParts)
    // read from sub partition table
    val rdd = tdw.table(srcTbl, priParts, subParts)
    // using tdwurl(e.g. "tdw://dbName/tblName/[priParts]/[subParts]")
    // val rdd = TDWFunctions.loadTable(sc, tdwurl)
    val cnt = rdd.count()
    println("record count: " + cnt)
    // save to tdw
//    tdw.saveToTable(rdd, tblName)
    // save to pri partition
//    tdw.saveToTable(rdd, tblName, priPartName)
    // save to sub partition
//    tdw.saveToTable(rdd, tblName, priPartName, subPartName)

    // using tdwurl(e.g. "tdw://dbName/tblName/[priPart]/[subPart]")
    rdd.saveToTable(tdwurl, overwrite = false)
  }
}
