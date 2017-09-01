package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by spenceryang
  * 2017/2/9.
  */

object FeedsClickRate {

  val tableName = "spenceryang_feeds_click_rate"

  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    tdwUtil.dropPartition(tableName,"p_"+dateStr)
    tdwUtil.createListPartition(tableName,"p_"+dateStr,dateStr)

    val kandianTdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    // extract article data with article id, first class id, and tag level equal to 16
    // tag level 16 is the first channel
    val articleInfoTable = kandianTdw.table("simsonhe_kandian_article_tag")
    val articleInfo = articleInfoTable.select("cms_article_id","tag_id", "tag_level").toDF("article_id","first_class_id", "tag_level")
    val articledata = articleInfo.filter(articleInfo("tag_level").equalTo(16))

    // extract all clicks and exposures
    // one article can have several sop type due to different uin
    // the number of one article of one particular type might be larger than 1
    // we care only care about the gender and age of the user while analysing click and exposure
    val splitHourInfoTable = kandianTdw.table("simsonhe_kandian_split_hour_info", Seq("p_" + dateStr))
    val record1 = splitHourInfoTable.select("sop_type", "article_id", "gender", "age").toDF("record_sop_type", "record_article_id", "record_gender", "record_age")
    val clickRecord = record1.filter(record1("record_sop_type").equalTo("0X80066FA")).select("record_article_id", "record_gender", "record_age")
    val exposureRecord = record1.filter(record1("record_sop_type").equalTo("0X80066FC")).select("record_article_id", "record_gender", "record_age")


    val pvJoinInfo = clickRecord.join(articledata, clickRecord("record_article_id") === articledata("article_id")).toDF().cache()
    val expJoinInfo = exposureRecord.join(articledata, exposureRecord("record_article_id") === articledata("article_id")).toDF().cache()

    val ageSegment: List[String] = List("11,16,12-15","15,19,16-18","18,24,19-23","23,31,24-30","30,41,31-40","40,51,41-50")
    val genderSegment: List[Long] = List(1, 2)

    for (item <- ageSegment) {
      val itemSplit = item.split(",")

      for (genderItem <- genderSegment) {

        val pvRecordTmp = pvJoinInfo.filter(pvJoinInfo("record_gender").equalTo(genderItem) && pvJoinInfo("record_age").gt(itemSplit.apply(0).toLong) && pvJoinInfo("record_age").lt(itemSplit.apply(1).toLong)).select("first_class_id").groupBy("first_class_id").count()

        val pvRecordTmp2 = pvRecordTmp.select(pvRecordTmp("first_class_id").as("pv_first_class_id"), pvRecordTmp("count").as("pv_count"))

        val exposureRecordTmp = expJoinInfo.filter(expJoinInfo("record_gender").equalTo(genderItem) && expJoinInfo("record_age").gt(itemSplit.apply(0).toLong) && expJoinInfo("record_age").lt(itemSplit.apply(1).toLong)).select("first_class_id").groupBy("first_class_id").count()

        val exposureRecordTmp2 = exposureRecordTmp.select(exposureRecordTmp("first_class_id").as("exposure_first_class_id"), exposureRecordTmp("count").as("exposure_count"))

        val joinRecord = pvRecordTmp2.join(exposureRecordTmp2, pvRecordTmp2("pv_first_class_id") === exposureRecordTmp2("exposure_first_class_id"))

        val joinRecordRate = joinRecord.withColumn("rate", joinRecord("pv_count") / joinRecord("exposure_count"))

        val joinRecordRate1 = joinRecordRate.withColumn("age", lit(itemSplit.apply(2))).withColumn("date_key", lit(dateStr.toLong)).withColumn("gender", lit(genderItem))

        kandianTdw.saveToTable(joinRecordRate1.select("date_key", "age", "gender","pv_first_class_id", "pv_count", "exposure_count", "rate"), tableName, "p_" + dateStr, null, false)

      }
    }
  }
}