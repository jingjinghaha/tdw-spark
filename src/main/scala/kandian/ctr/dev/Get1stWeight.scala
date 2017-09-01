package com.tencent.kandian.ctr.dev

import com.tencent.kandian.ctr.dev.Get2ndWeight.getTagArray
import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil, TableDesc}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.util.parsing.json.JSON

/**
  * Created by franniewu on 2017/5/18.
  */
object Get1stWeight {
  class ElementContainer[T] { def unapply(element:Any):Option[T] = Some(element.asInstanceOf[T]) }
  object MapContainer extends ElementContainer[Map[String, Any]]
  object ListContainer extends ElementContainer[List[Any]]
  object StringContainer extends ElementContainer[String]
  object DoubleContainer extends ElementContainer[Double]
  object IntContainer extends ElementContainer[Int]

  def main(args: Array[String]): Unit ={
    val tableName = "franniewu_1st_channel_weight"
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()
    val userProfileTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val userProfileTable = userProfileTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + dateStr))
    val userProfileData = userProfileTable.select("tdbank_imp_date", "uin", "potrait").toDF()

    def getTagArray(jsonString: String) : Array[Double] = {
      val userTagArray = new Array[Double](61)
      try {
        val userTagList = for {
          Some(MapContainer(userMap)) <- List(JSON.parseFull(jsonString))
          ListContainer(tagList) = userMap("channel_model")
          MapContainer(tagMap) <- tagList
          DoubleContainer(tagIdx) = tagMap("id")
          DoubleContainer(tagWeight) = tagMap("weight")
        } yield {
          (tagIdx, tagWeight)
        }
        for ((userTagIdx, userTagWeight) <- userTagList) {
          userTagArray(userTagIdx.toInt) = userTagWeight
        }
      } catch {
        case ex: Throwable =>
      }
      userTagArray
    }

    val userTagSchema = StructType(
      userProfileData.schema.fields ++
        Seq(
          StructField("id0", DoubleType), StructField("id1", DoubleType), StructField("id2", DoubleType),
          StructField("id3", DoubleType), StructField("id4", DoubleType), StructField("id5", DoubleType),
          StructField("id6", DoubleType), StructField("id7", DoubleType), StructField("id8", DoubleType),
          StructField("id9", DoubleType), StructField("id10", DoubleType), StructField("id11", DoubleType),
          StructField("id12", DoubleType), StructField("id13", DoubleType), StructField("id14", DoubleType),
          StructField("id15", DoubleType), StructField("id16", DoubleType), StructField("id17", DoubleType),
          StructField("id18", DoubleType), StructField("id19", DoubleType), StructField("id20", DoubleType),
          StructField("id21", DoubleType), StructField("id22", DoubleType), StructField("id23", DoubleType),
          StructField("id24", DoubleType), StructField("id25", DoubleType), StructField("id26", DoubleType),
          StructField("id27", DoubleType), StructField("id28", DoubleType), StructField("id29", DoubleType),
          StructField("id30", DoubleType), StructField("id31", DoubleType), StructField("id32", DoubleType),
          StructField("id33", DoubleType), StructField("id34", DoubleType), StructField("id35", DoubleType),
          StructField("id36", DoubleType), StructField("id37", DoubleType), StructField("id38", DoubleType),
          StructField("id39", DoubleType), StructField("id40", DoubleType), StructField("id41", DoubleType),
          StructField("id42", DoubleType), StructField("id43", DoubleType), StructField("id44", DoubleType),
          StructField("id45", DoubleType), StructField("id46", DoubleType)
        ))
    val userProfileRows = userProfileData.rdd.map(r => (r.get(1), r)).reduceByKey((r0, r1) => r0).map(r => r._2)
      .map(r => Row.fromSeq(r.toSeq ++ getTagArray(r.getAs[String]("potrait"))))
    val userProfileDF = spark.createDataFrame(userProfileRows, userTagSchema)

    val output = userProfileDF.select("tdbank_imp_date", "uin",
      "id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46")

//    createTable(tableName,user,pwd,dateStr)
    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tableName,"p_"+dateStr)
    tdwUtil.createListPartition(tableName,"p_"+dateStr,dateStr)
    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    tdw.saveToTable(output,tableName,"p_"+dateStr)
  }

  def createTable(tableName:String,user:String,pwd:String,dateStr:String): Unit ={
    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    val tableDesc = new TableDesc().setTblName("franniewu_1st_channel_weight")
      .setCols(Seq(
        Array("date_key","String","the date"),
        Array("uin","BigInt","the user id"),
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
      .setComment("this is a table to record 1st channel weight")
      .setCompress(true)
      .setFileFormat("RCFILE")
      .setPartType("LIST")
      .setPartField("date_key")
    tdwUtil.createTable(tableDesc)
    tdwUtil.createListPartition("franniewu_1st_channel_weight", "p_"+dateStr, dateStr)
  }
}
