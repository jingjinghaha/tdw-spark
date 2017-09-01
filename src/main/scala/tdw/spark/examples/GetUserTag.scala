package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2017/5/3.
  */
object GetUserTag {
  val tableName = "haohaochen_user_potrait_tag"

  def main(args:Array[String]) = {
    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tableName,"p_"+dateStr)
    tdwUtil.createListPartition(tableName,"p_"+dateStr,dateStr)

    val userPotraitTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val userPotraitTable = userPotraitTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + dateStr))
    val userPotraitJsonData = userPotraitTable.select("tdbank_imp_date", "uin", "potrait").toDF()

    val userPotraitJsonDataRowsGetKeyUin = userPotraitJsonData.rdd.map(r => (r.get(1), r))
    val userPotraitJsonDataRowsReduceByKeyUin = userPotraitJsonDataRowsGetKeyUin.reduceByKey((r0, r1) => r0).map(r => r._2)
    //val userPotraitJsonDataDF = spark.createDataFrame(userPotraitRowsReduceByKeyUin, userTagSchema)

    class ElementContainer[T] { def unapply(element:Any):Option[T] = Some(element.asInstanceOf[T]) }
    object MapContainer extends ElementContainer[Map[String, Any]]
    object ListContainer extends ElementContainer[List[Any]]
    object StringContainer extends ElementContainer[String]
    object DoubleContainer extends ElementContainer[Double]
    object IntContainer extends ElementContainer[Int]

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
      userPotraitJsonData.schema.fields ++
      Array(
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
        StructField("id45", DoubleType), StructField("id46", DoubleType), StructField("id47", DoubleType),
        StructField("id48", DoubleType), StructField("id49", DoubleType), StructField("id50", DoubleType),
        StructField("id51", DoubleType), StructField("id52", DoubleType), StructField("id53", DoubleType),
        StructField("id54", DoubleType), StructField("id55", DoubleType), StructField("id56", DoubleType),
        StructField("id57", DoubleType), StructField("id58", DoubleType), StructField("id59", DoubleType),
        StructField("id60", DoubleType)
      ))
    val userPotraitRows = userPotraitJsonDataRowsReduceByKeyUin.map(r => Row.fromSeq(r.toSeq ++ getTagArray(r.getAs[String]("potrait"))))
    val userPotraitDF = spark.createDataFrame(userPotraitRows, userTagSchema)



    val kandianTdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    kandianTdw.saveToTable(userPotraitDF.select( "tdbank_imp_date", "uin",
      "id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46", "id47", "id48", "id49",
      "id50", "id51", "id52", "id53", "id54", "id55", "id56", "id57", "id58", "id59",
      "id60"), tableName, "p_" + dateStr)

  }


}
