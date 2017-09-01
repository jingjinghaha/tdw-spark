package com.tencent.tdw.spark.examples

import org.apache.spark.sql.SparkSession

/**
 * Created by sharkdtu
 * 2016/9/21.
 */
object TDWDataSetTest {
  case class People(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val people = spark.read.json("hdfs://host:port/path/to/people.json").as[People]
    people.groupBy("name").count()
    spark.stop()
  }
}
