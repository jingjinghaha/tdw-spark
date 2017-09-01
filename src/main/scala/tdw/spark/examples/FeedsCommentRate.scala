package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by spenceryang
  * 2017/2/9.
  */

object FeedsCommentRate {

  val tableName = "spenceryang_feeds_comment_rate"

  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tableName, "p_" + dateStr)
    tdwUtil.createListPartition(tableName, "p_" + dateStr, dateStr)

    val userTdw = new TDWSQLProvider(spark, user, pwd, "imdataoss")
    val userTable = userTdw.table("im_data_02_04_091_monthly", DateUtil.getInfoPartSeq(dateStr))
    val record3 = userTable.select("uin", "age", "gender", "recent_login_count")
    val userRecord = record3.filter(record3("recent_login_count").>(0)).select("uin", "age", "gender").toDF("uin", "record_age", "record_gender")

    val kandianTdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    // extract article data with article id, first class id, and tag level equal to 16
    // the first class id is just the tag id!!!!!
    val articleInfoTable = kandianTdw.table("simsonhe_kandian_article_tag")
    val articleInfo = articleInfoTable.select("cms_article_id","source_article_id", "tag_id", "tag_level").toDF("cms_article_id", "source_article_id", "first_class_id", "tag_level")
    val articledata = articleInfo.filter(articleInfo("tag_level").equalTo(16))

    // extract all clicks and exposures
    // one article can have several sop type due to different uin
    // the number of one article of one particular type might be larger than 1
    // we care only care about the gender and age of the user while analysing click and exposure
    val splitHourInfoTable = kandianTdw.table("simsonhe_kandian_split_hour_info", Seq("p_" + dateStr))
    val record1 = splitHourInfoTable.select("sop_type", "article_id", "gender", "age").toDF("record_sop_type", "record_article_id", "record_gender", "record_age")
    val clickRecord = record1.filter(record1("record_sop_type").equalTo("0X80066FA")).select("record_article_id", "record_gender", "record_age")
    //    val exposureRecord = record1.filter(record1("record_sop_type").equalTo("0X80066FC")).select("record_article_id", "record_gender", "record_age")

    // the record type
    val detailTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val optTable = detailTdw.table("gongzhonghao_dsl_comment_data_fht0", DateUtil.getOneDaySeq(dateStr))
    val record2 = optTable.select("type", "article_id", "cuin").toDF("type", "article_id", "uin")
    val commentRecord = record2.filter(record2("type").equalTo(2)).select("article_id", "uin").toDF("article_id", "uin")

    val pvJoinInfo = clickRecord.join(articledata, clickRecord("record_article_id") === articledata("cms_article_id")).toDF().cache()
    val commentJoinInfo = commentRecord.join(articledata, commentRecord("article_id") === articledata("source_article_id")).join(userRecord, commentRecord("uin") === userRecord("uin")).toDF().cache()

    val ageSegment: List[String] = List("11,16,12-15", "15,19,16-18", "18,24,19-23", "23,31,24-30", "30,41,31-40","40,51,41-50")
    val genderSegment: List[Long] = List(1, 2)

    for (item <- ageSegment) {
      val itemSplit = item.split(",")

      for (genderItem <- genderSegment) {

        val pvRecordTmp = pvJoinInfo.filter(pvJoinInfo("record_gender").equalTo(genderItem) && pvJoinInfo("record_age").gt(itemSplit.apply(0).toLong) && pvJoinInfo("record_age").lt(itemSplit.apply(1).toLong)).select("first_class_id").groupBy("first_class_id").count()

        val pvRecordTmp2 = pvRecordTmp.select(pvRecordTmp("first_class_id").as("pv_first_class_id"), pvRecordTmp("count").as("pv_count"))

        val commentRecordTmp = commentJoinInfo.filter(commentJoinInfo("record_gender").equalTo(genderItem) && commentJoinInfo("record_age").gt(itemSplit.apply(0).toLong) && commentJoinInfo("record_age").lt(itemSplit.apply(1).toLong)).select("first_class_id").groupBy("first_class_id").count()

        val commentRecordTmp2 = commentRecordTmp.select(commentRecordTmp("first_class_id").as("comment_first_class_id"), commentRecordTmp("count").as("comment_count"))

        val joinRecord = pvRecordTmp2.join(commentRecordTmp2, pvRecordTmp2("pv_first_class_id") === commentRecordTmp2("comment_first_class_id"))

        val joinRecordRate = joinRecord.withColumn("rate", joinRecord("comment_count") / joinRecord("pv_count"))

        val joinRecordRate1 = joinRecordRate.withColumn("age", lit(itemSplit.apply(2))).withColumn("date_key", lit(dateStr.toLong)).withColumn("gender", lit(genderItem))

        kandianTdw.saveToTable(joinRecordRate1.select("date_key", "age", "gender", "pv_first_class_id", "comment_count", "pv_count", "rate"), tableName, "p_" + dateStr, null, false)

      }
    }
  }
}