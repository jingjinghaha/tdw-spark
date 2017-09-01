package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil, TableDesc}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.parsing.json.JSON

/**
  * Created by franniewu on 2017/5/18.
  */
object Get2ndWeight {

  class ElementContainer[T] { def unapply(element:Any):Option[T] = Some(element.asInstanceOf[T]) }
  object MapContainer extends ElementContainer[Map[String, Any]]
  object ListContainer extends ElementContainer[List[Any]]
  object StringContainer extends ElementContainer[String]
  object DoubleContainer extends ElementContainer[Double]
  object IntContainer extends ElementContainer[Int]

  def main(args: Array[String]): Unit ={

    val tableName = "franniewu_2nd_channel_weight_women"
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()
    val userProfileTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val userProfileTable = userProfileTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + dateStr))
    val userProfileData = userProfileTable.select("tdbank_imp_date", "uin", "potrait").toDF()

    val userTagSchema = StructType(userProfileData.schema.fields ++
        Seq(
          StructField("id0", DoubleType), StructField("id1", DoubleType), StructField("id2", DoubleType),
          StructField("id3", DoubleType), StructField("id4", DoubleType), StructField("id5", DoubleType),
          StructField("id6", DoubleType), StructField("id7", DoubleType), StructField("id8", DoubleType),
          StructField("id9", DoubleType), StructField("id10", DoubleType), StructField("id11", DoubleType),
          StructField("id12", DoubleType), StructField("id13", DoubleType), StructField("id14", DoubleType),
          StructField("id15", DoubleType), StructField("id16", DoubleType), StructField("id17", DoubleType),
          StructField("id18", DoubleType), StructField("id19", DoubleType), StructField("id20", DoubleType),
          StructField("id21", DoubleType), StructField("id22", DoubleType), StructField("id23", DoubleType),
          StructField("id24", DoubleType)
        )
    )
    val userProfileRows = userProfileData.rdd.map(r => (r.get(1), r)).reduceByKey((r0, r1) => r0).map(r => r._2)
      .map(r => Row.fromSeq(r.toSeq ++ getTagArray(r.getAs[String]("potrait"))))
    val userProfileDF = spark.createDataFrame(userProfileRows, userTagSchema).cache()
    userProfileDF.show(5)

    val finalDF = userProfileDF.filter(
      userProfileDF("id0")>0 || userProfileDF("id1")>0 || userProfileDF("id2")>0 ||
        userProfileDF("id3")>0 || userProfileDF("id4")>0 || userProfileDF("id5")>0 ||
        userProfileDF("id6")>0 || userProfileDF("id7")>0 || userProfileDF("id8")>0 ||
        userProfileDF("id9")>0 || userProfileDF("id10")>0 || userProfileDF("id11")>0 ||
        userProfileDF("id12")>0 || userProfileDF("id13")>0 || userProfileDF("id14")>0 ||
        userProfileDF("id15")>0 || userProfileDF("id16")>0 || userProfileDF("id17")>0 ||
        userProfileDF("id18")>0 || userProfileDF("id19")>0 || userProfileDF("id20")>0 ||
        userProfileDF("id21")>0 || userProfileDF("id22")>0 || userProfileDF("id23")>0 ||
        userProfileDF("id24")>0
    ).cache()
    println(finalDF.count())

    val mediaTdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
//    createTable(tableName, user, pwd, dateStr)
    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tableName,"p_"+dateStr)
    tdwUtil.createListPartition(tableName,"p_"+dateStr,dateStr)
    mediaTdw.saveToTable(finalDF.select("tdbank_imp_date","uin","id0", "id1", "id2", "id3", "id4",
      "id5", "id6", "id7", "id8", "id9", "id10", "id11", "id12", "id13", "id14", "id15", "id16",
      "id17", "id18", "id19", "id20", "id21", "id22", "id23", "id24"),
      "franniewu_2nd_channel_weight_women","p_"+dateStr)

  }

  def createTable(tableName: String, user: String, pwd: String, dateStr: String): Unit ={
    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")

    val tableDesc = new TableDesc().setTblName(tableName)
      .setCols(Seq(
        Array("date_key","String","the date"),
        Array("uin","BigInt","the user id"),
        Array("id0","Double","second channel weight"),
        Array("id1","Double","second channel weight"),
        Array("id2","Double","second channel weight"),
        Array("id3","Double","second channel weight"),
        Array("id4","Double","second channel weight"),
        Array("id5","Double","second channel weight"),
        Array("id6","Double","second channel weight"),
        Array("id7","Double","second channel weight"),
        Array("id8","Double","second channel weight"),
        Array("id9","Double","second channel weight"),
        Array("id10","Double","second channel weight"),
        Array("id11","Double","second channel weight"),
        Array("id12","Double","second channel weight"),
        Array("id13","Double","second channel weight"),
        Array("id14","Double","second channel weight"),
        Array("id15","Double","second channel weight"),
        Array("id16","Double","second channel weight"),
        Array("id17","Double","second channel weight"),
        Array("id18","Double","second channel weight"),
        Array("id19","Double","second channel weight"),
        Array("id20","Double","second channel weight"),
        Array("id21","Double","second channel weight"),
        Array("id22","Double","second channel weight"),
        Array("id23","Double","second channel weight"),
        Array("id24","Double","second channel weight")
      ))
      .setComment("this is a table to record 2nd channel weight for meizhuang users")
      .setCompress(true)
      .setFileFormat("RCFILE")
      .setPartType("LIST")
      .setPartField("date_key")
    tdwUtil.createTable(tableDesc)
    tdwUtil.createListPartition(tableName, "p_"+dateStr, dateStr)
  }

  def get2ndChannelMap(int: Int): Map[Int,Int] = {
    val map: Map[Int,Int] = Map(
      501 -> 0,
      502 -> 1,
      503 -> 2,
      504	-> 3,
      505	-> 4,
      506	-> 5,
      507	-> 6,
      508	-> 7,
      509	-> 8,
      510	-> 9,
      511	-> 10,
      520	-> 11,
      521	-> 12,
      522	-> 13,
      523	-> 14,
      524	-> 15,
      525	-> 16,
      526	-> 17,
      527	-> 18,
      528	-> 19,
      529	-> 20,
      530	-> 21,
      531	-> 22,
      532	-> 23,
      533	-> 24
    )
    map
  }

  def getTagArray(jsonString: String) : Array[Double] = {
    val userTagArray = new Array[Double](25)
    try {
      println(jsonString)
      val userTagList =
        for {
          Some(MapContainer(userMap)) <- List(JSON.parseFull(jsonString))
          ListContainer(tagList) = userMap("channel_model_beta")
          MapContainer(tagMap) <- tagList
          DoubleContainer(tagIdx) = tagMap("id")
          DoubleContainer(tagWeight) = tagMap("weight")
        } yield {
          (tagIdx, tagWeight)
        }
//      List()
//      List()
//      List((312.0,0.8286869525909424), (1501.0,0.4642404913902283), (1502.0,0.3659418523311615), (306.0,0.02206454612314701))
//      List((312.0,0.8286869525909424), (1501.0,0.4642404913902283), (1502.0,0.3659418523311615), (306.0,0.02206454612314701))
      println(userTagList)
      println(userTagList.toString())
      for ((userTagIdx, userTagWeight) <- userTagList) {
        val userTagIdxNew = get2ndChannelMap(0).apply(userTagIdx.toInt)
        userTagArray(userTagIdxNew.toInt) = userTagWeight
        // the print info is in the log of excutors
//        println(userTagIdxNew)
      }
    } catch {
      case ex: Throwable =>
    }
    println(userTagArray)
    userTagArray
  }
}
