package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil, TableDesc}

/**
  * Created by franniewu on 2017/5/16.
  */
object ClusteringAnalysis {

  def main(args: Array[String]): Unit ={
    val tableName = "franniewu_1st_channel_clustering_analysis"
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().getOrCreate()
    val tdw_media = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    val clusteringResult = tdw_media.table("franniewu_1st_channel_clustering")
      .select("date_key","uin","label").toDF("date_key1","uin1","label")
    val clusteringResult_new = clusteringResult.filter(clusteringResult("date_key1") === dateStr)
    println(clusteringResult_new.count())

    val weights = tdw_media.table("franniewu_1st_channel_weight",Seq("p_"+dateStr))
    println(weights.count())

    val combined = clusteringResult_new.join(weights, clusteringResult_new("uin1") === weights("uin"))
    combined.show(5)

    val sum = combined.groupBy("label").sum("id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46").toDF("label","id0", "id1", "id2", "id3", "id4",
      "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46")
    sum.show()

    val mean = combined.groupBy("label").mean("id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46").toDF("label","id0", "id1", "id2", "id3", "id4",
      "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46")
    mean.show()

    val avg = combined.groupBy("label").avg("id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46").toDF("label","id0", "id1", "id2", "id3", "id4",
      "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46")
    avg.show()

    val max = combined.groupBy("label").max("id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46").toDF("label","id0", "id1", "id2", "id3", "id4",
      "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46")
    max.show()

    val min = combined.groupBy("label").min("id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46").toDF("label","id0", "id1", "id2", "id3", "id4",
      "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46")
    min.show()

    val df = sum.union(mean).union(avg).union(max).union(min)
    df.show()
    df.printSchema()
    val output = df.withColumn("date_key",lit(dateStr))
    df.printSchema()

//   createTable(tableName,user,pwd,dateStr)
    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tableName,"p_"+dateStr)
    tdwUtil.createListPartition(tableName, "p_"+dateStr, dateStr)
    tdw_media.saveToTable(output.select("label","date_key",
      "id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46"),tableName,"p_"+dateStr)

  }

  def createTable(tableName: String, user: String, pwd: String, dateStr: String): Unit ={
    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")

    val tableDesc = new TableDesc().setTblName(tableName)
      .setCols(Seq(
        Array("label","Int","the user cluster id"),
        Array("date_key","String","the date"),
        Array("id0","Double","first channel weight"),
        Array("id1","Double","first channel weight"),
        Array("id2","Double","first channel weight"),
        Array("id3","Double","first channel weight"),
        Array("id4","Double","first channel weight"),
        Array("id5","Double","first channel weight"),
        Array("id6","Double","first channel weight"),
        Array("id7","Double","first channel weight"),
        Array("id8","Double","first channel weight"),
        Array("id9","Double","first channel weight"),
        Array("id10","Double","first channel weight"),
        Array("id11","Double","first channel weight"),
        Array("id12","Double","first channel weight"),
        Array("id13","Double","first channel weight"),
        Array("id14","Double","first channel weight"),
        Array("id15","Double","first channel weight"),
        Array("id16","Double","first channel weight"),
        Array("id17","Double","first channel weight"),
        Array("id18","Double","first channel weight"),
        Array("id19","Double","first channel weight"),
        Array("id20","Double","first channel weight"),
        Array("id21","Double","first channel weight"),
        Array("id22","Double","first channel weight"),
        Array("id23","Double","first channel weight"),
        Array("id24","Double","first channel weight"),
        Array("id25","Double","first channel weight"),
        Array("id26","Double","first channel weight"),
        Array("id27","Double","first channel weight"),
        Array("id28","Double","first channel weight"),
        Array("id29","Double","first channel weight"),
        Array("id30","Double","first channel weight"),
        Array("id31","Double","first channel weight"),
        Array("id32","Double","first channel weight"),
        Array("id33","Double","first channel weight"),
        Array("id34","Double","first channel weight"),
        Array("id35","Double","first channel weight"),
        Array("id36","Double","first channel weight"),
        Array("id37","Double","first channel weight"),
        Array("id38","Double","first channel weight"),
        Array("id39","Double","first channel weight"),
        Array("id40","Double","first channel weight"),
        Array("id41","Double","first channel weight"),
        Array("id42","Double","first channel weight"),
        Array("id43","Double","first channel weight"),
        Array("id44","Double","first channel weight"),
        Array("id45","Double","first channel weight"),
        Array("id46","Double","first channel weight")
      ))
      .setComment("this is a table to analysis the 1st channel clustering result")
      .setCompress(true)
      .setFileFormat("RCFILE")
      .setPartType("LIST")
      .setPartField("date_key")
    tdwUtil.createTable(tableDesc)
    tdwUtil.createListPartition(tableName, "p_"+dateStr, dateStr)
  }
  //    val spark = SparkSession.builder().getOrCreate()
  //    import spark.implicits._
  //    case class Record(id0:Double, id1:Double, id2:Double, id3:Double, id4:Double, id5:Double,
  //                      id6:Double, id7:Double, id8:Double, id9:Double, id10:Double, id11:Double,
  //                      id12:Double, id13:Double, id14:Double, id15:Double, id16:Double, id17:Double,
  //                      id18:Double, id19:Double, id20:Double, id21:Double, id22:Double, id23:Double,
  //                      id24:Double, id25:Double, id26:Double, id27:Double, id28:Double, id29:Double,
  //                      id30:Double, id31:Double, id32:Double, id33:Double, id34:Double, id35:Double,
  //                      id36:Double, id37:Double, id38:Double, id39:Double, id40:Double, id41:Double,
  //                      id42:Double, id43:Double, id44:Double, id45:Double, id46:Double, label: Int)
  //    val myFile = sc.textFile(path)
  //    val myFile1 = myFile.map(x => x.split(" ",-1)).map{
  //      case Array(id0,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19,id20,id21,id22,id23,id24,id25,id26,id27,id28,id29,id30,id31,id32,id33,id34,id35,id36,id37,id38,id39,id40,id41,id42,id43,id44,id45,id46,label)
  //       =>
  //        Record(id0.toDouble, id1.toDouble, id2.toDouble, id3.toDouble, id4.toDouble,
  //          id5.toDouble, id6.toDouble, id7.toDouble, id8.toDouble, id9.toDouble,
  //          id10.toDouble, id11.toDouble, id12.toDouble, id13.toDouble, id14.toDouble,
  //          id15.toDouble, id16.toDouble, id17.toDouble, id18.toDouble, id19.toDouble,
  //          id20.toDouble, id21.toDouble, id22.toDouble, id23.toDouble, id24.toDouble,
  //          id25.toDouble, id26.toDouble, id27.toDouble, id28.toDouble, id29.toDouble,
  //          id30.toDouble, id31.toDouble, id32.toDouble, id33.toDouble, id34.toDouble,
  //          id35.toDouble, id36.toDouble, id37.toDouble, id38.toDouble, id39.toDouble,
  //          id40.toDouble, id41.toDouble, id42.toDouble, id43.toDouble, id44.toDouble,
  //          id45.toDouble, id46.toDouble, label.toInt)
  //    }

  //    val df = myFile1.toDF()

}
