package com.tencent.tdw.spark.examples

import java.util.Properties

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import org.apache.spark.sql.SparkSession

/**
 * Created by sharkdtu
 * 2016/10/11.
 */
object DBDataFrameTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)

    val dbUrl = cmdArgs.getStringValue("url")
    val table = cmdArgs.getStringValue("table")

    val props = new Properties()
    props.setProperty("user", cmdArgs.getStringValue("user"))
    props.setProperty("password", cmdArgs.getStringValue("password"))


    val df = spark.read.jdbc(dbUrl, table, props)
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

    df.write.jdbc(dbUrl, table, props)
  }
}
