package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWFunctions._
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.sql.SparkSession

/**
 * Created by sharkdtu
 * 2015/9/1.
 */

object TDWDataFrameTest {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)

    val srcTbl = cmdArgs.getStringValue("src_tbl")
    val dstTbl = cmdArgs.getStringValue("dst_tbl")
    val priParts = cmdArgs.getCommaSplitArrayValue("pri_parts")
    val subParts = cmdArgs.getCommaSplitArrayValue("sub_parts")

    val tdw = new TDWSQLProvider(spark, "test_user", "test_passwd", "test_db")
    val df = tdw.table(srcTbl, priParts, subParts).cache()
    // using tdwurl(e.g. "tdw://dbName/tblName/[priParts]/[subParts]")
    // val df = TDWFunctions.loadTable(spark, tdwurl)

    /**
     * print schema:
     * root
     * |-- a: byte (nullable = true)
     * |-- b: short (nullable = true)
     * |-- c: integer (nullable = true)
     * |-- d: long (nullable = true)
     * |-- e: boolean (nullable = true)
     * |-- f: float (nullable = true)
     * |-- g: double (nullable = true)
     * |-- h: string (nullable = true)
     */
    df.printSchema()
    // default show 20 records
    df.show()
    // rename column
    val ab = df.select("a", "b").toDF("ab_a", "ab_b").cache()
    ab.filter(ab("ab_a") > 1).show()
    //join ab and ac
    val ac = df.select("a", "c").toDF("ac_a", "ac_c")
    ab.join(ac, ab("ab_a") === ac("ac_a")).show()
    ab.join(ac, ab("ab_a") === ac("ac_a"), "left_outer").show()
    //Compute the max "b" and average "c", grouped by "a".
    df.groupBy("a").agg(Map(
      "b" -> "max",
      "c" -> "avg",
      "d" -> "sum"
    )).show()

    tdw.saveToTable(df.select("a", "b", "c"), dstTbl)
    // df.saveToTable(tdwurl)
  }
}