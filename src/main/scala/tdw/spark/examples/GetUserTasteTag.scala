package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2017/5/3.
  */
object GetUserTasteTag {

  val tableName = "haohaochen_user_taste_comic_tags"

  def main(args:Array[String]) = {
    val spark = SparkSession.builder().getOrCreate()


    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tableName,"p_"+dateStr)
    tdwUtil.createListPartition(tableName,"p_"+dateStr,dateStr)

    val kandianTdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val articleComicTagsData = kandianTdw.table("haohaochen_article_comic_tags", Seq("p_" + dateStr))
    val articleComicTagsDataSet = articleComicTagsData.selectExpr("cast(comic_tag_id as int) comic_tag_id").rdd.map(r => r.get(0)).collect().toSet

    val userPotraitTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val userPotraitTable = userPotraitTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + dateStr))
    val userPotraitJsonData = userPotraitTable.select("tdbank_imp_date", "uin", "potrait").toDF()

    val userPotraitJsonDataRowsGetKeyUin = userPotraitJsonData.rdd.map(r => (r.get(1), r))
    val userPotraitJsonDataRowsReduceByKeyUin = userPotraitJsonDataRowsGetKeyUin.reduceByKey((r0, r1) => r0).map(r => r._2)

    class ElementContainer[T] { def unapply(element:Any):Option[T] = Some(element.asInstanceOf[T]) }
    object MapContainer extends ElementContainer[Map[String, Any]]
    object ListContainer extends ElementContainer[List[Any]]
    object StringContainer extends ElementContainer[String]
    object DoubleContainer extends ElementContainer[Double]
    object IntContainer extends ElementContainer[Int]
    def cntTasteComicTags(jsonString: String) : Int = {
      var cntTags: Int = 0
      try {
        val userTagList = for {
          Some(MapContainer(userMap)) <- List(JSON.parseFull(jsonString))
          ListContainer(tagList) = userMap("tag_model")
          MapContainer(tagMap) <- tagList
          DoubleContainer(tagIdx) = tagMap("id")
          DoubleContainer(tagWeight) = tagMap("weight")
        } yield {
          (tagIdx, tagWeight)
        }
        for ((userTagIdx, userTagWeight) <- userTagList) {
          if(articleComicTagsDataSet(userTagIdx.toInt)) {
            cntTags += 1
          }
        }
      } catch {
        case ex: Throwable =>
      }
      return cntTags
    }
    val userTasteTagSchema = StructType(userPotraitJsonData.schema.fields ++ Array(StructField("comic_tags_cnt", IntegerType)))
    val userTasteTagsRows = userPotraitJsonDataRowsReduceByKeyUin.map(r => Row.fromSeq(r.toSeq ++ Seq(cntTasteComicTags(r.getAs[String]("potrait")))))
    val userTasteTagsDF = spark.createDataFrame(userTasteTagsRows, userTasteTagSchema)

    kandianTdw.saveToTable(userTasteTagsDF.select( "tdbank_imp_date", "uin", "potrait", "comic_tags_cnt"), tableName, "p_" + dateStr)
  }
}
