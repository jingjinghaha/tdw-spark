package com.tencent.tdw.spark.examples

import org.apache.spark.{SparkContext, SparkConf}
import com.tencent.tdw.spark.toolkit.tdw.{TableDesc, TDWUtil}

/**
 * Created by sharkdtu
 * 2015/9/1.
 */
object TDWTableDDLTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val tdwUtil = new TDWUtil("test_user", "test_passwd", "test_db")
    //create table
    val tableDesc = new TableDesc().setTblName("test")
      .setCols(
        Seq(Array("col1", "bigint", "this is col1"),
          Array("col2", "int", "this is col2"),
          Array("col3", "string", "this is col3")
        ))
      .setComment("this is a test table")
      .setCompress(true)
      .setFileFormat("RCFILE")
      .setPartType("LIST")
      .setPartField("col1")
    tdwUtil.createTable(tableDesc)

    //create list partition
    tdwUtil.createListPartition("test", "p_20150930", "20150930")
    sc.stop()
  }
}
