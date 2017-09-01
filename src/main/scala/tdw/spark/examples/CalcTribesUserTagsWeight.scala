package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Administrator on 2017/6/12.
  */
object CalcTribesUserTagsWeight {
  val tribeTagsInfoTableName = "haohaochen_tribe_tags_info"
  val tribeUserTagsTableName = "haohaochen_tribe_user_tags_info"
  val tribeUserInfoTableName = "group_tribe_user_followed_tribes_for_kandian"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sparkContext = new SparkContext(conf)

    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tribeUserTagsTableName,"p_"+dateStr)
    tdwUtil.createListPartition(tribeUserTagsTableName,"p_"+dateStr,dateStr)

    val kandianTdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val tribeTdw = new TDWSQLProvider(spark, user, pwd, "sng_imappdev_tribe_app")
    val tribeInfo = kandianTdw.table(tribeTagsInfoTableName, Seq("p_" + dateStr))
      .selectExpr("cast(tribe_name as string)tribe_name", "cast(tribe_num as long)tribe_num", "cast(tribe_name_tag as string)stribe_name_tag", "cast(tribe_info_tag as string)tribe_info_tag")
      .rdd.flatMap(r => {
        val tribe_name_tags_array = r.getString(2).split("\\|").distinct.filter(r => (r != null && r != "")).map(r => (r, 1))
        val tribe_info_tags_array = r.getString(3).split("\\|").distinct.filter(r => (r != null && r != "")).map(r => (r, 2))
        val tribe_tags_array = tribe_info_tags_array ++ tribe_name_tags_array
        tribe_tags_array.map(tag => (r.getString(0), tag))
      })
      .map(r => (r._1, Seq(r._2)))
      .distinct()
      .reduceByKey((r0, r1) => r0 ++ r1)
      .collectAsMap()
    println("the head:" + tribeInfo.head)
    val tribeInfoBroadcast = sparkContext.broadcast(tribeInfo)

    val tribeUserInfo = tribeTdw.table(tribeUserInfoTableName, Seq("p_" + dateStr))
      .selectExpr("cast(uin as long)uin", "cast(tribes as string)tribes")
      .rdd.flatMap(r => {
        val tribes_array = r.getString(1).split(" ")
        tribes_array.map(tribe => (r.getLong(0), tribe))
      }).repartition(600)


    val tribeUserTagsInfo = tribeUserInfo
      .map(r => {
        val tribeInfoMap = tribeInfoBroadcast.value
        if (tribeInfoMap.contains(r._2)) {
          (r._1, tribeInfoMap.get(r._2).getOrElse(Seq[(String, Int)]()))
        } else {
          (r._1, Seq[(String, Int)]())
        }
      })
      .flatMap(r => {
        val tagSeq = r._2
        tagSeq.map(tag => Row(dateStr.toLong, r._1, tag._1, tag._2))
      })

    val tribeUserSchema = StructType(
      Array(StructField("date_key", LongType), StructField("uin", LongType), StructField("tag", StringType), StructField("tag_level", IntegerType))
    )
    val tribeUserTagsDf = spark.createDataFrame(tribeUserTagsInfo, tribeUserSchema)

    kandianTdw.saveToTable(tribeUserTagsDf, tribeUserTagsTableName, "p_" + dateStr)
  }
}
