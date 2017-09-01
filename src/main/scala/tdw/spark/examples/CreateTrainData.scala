package com.tencent.tdw.spark.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import net.liftweb.json._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CreateTrainData {

  def main(args: Array[String]) {

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    //info分区 取上个月的数据
    val df = new SimpleDateFormat("yyyyMMdd")
    val dfs = new SimpleDateFormat("yyyyMM")

    val date = df.parse(dateStr)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)
    val dayIndex = calendar.get(Calendar.DAY_OF_WEEK)
    calendar.add(Calendar.DAY_OF_MONTH, -10)
    calendar.add(Calendar.MONTH, -1)

    //val spark = SparkSession.builder().config("spark.driver.maxResultSize","2g").getOrCreate()
    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val tdw2 = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")

    val userActiveTable = tdw.table("simsonhe_kandian_user_active", Seq("p_" + dateStr))
    //val filterUser = userActiveTable.filter(userActiveTable("active_level") > 0.8).select("uin", "active_level")
    val filterUser = userActiveTable.select("uin", "active_level")

    val recordTable = tdw.table("simsonhe_kandian_split_hour_info", Seq("p_" + dateStr))
    val record1 = recordTable.select("date_key", "sop_type", "article_id", "gender", "age", "ext_info", "city_id", "uin", "platform").toDF("record_date_key", "record_sop_type", "record_article_id", "record_gender", "record_age", "record_ext_info", "record_city_id", "record_uin", "record_platform")
    val record2 = record1.filter(record1("record_sop_type").equalTo("0X80066FA") || record1("record_sop_type").equalTo("0X8007626"))

    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin"
    val userXrecord = filterUser.join(record2, filterUser("uin") === record2("record_uin"))

    val exposureRecord = userXrecord.filter(userXrecord("record_sop_type").equalTo("0X8007626"))

    val getBehavior: ((String, String) => String) = (platform: String, extInfo: String) => {

      var behaviorType = 0
      try {
        val json = parse(extInfo).asInstanceOf[JObject]
        behaviorType = json.values("behavior_type").toString.toInt
      }
      catch {
        case ex: Exception => behaviorType = -1
      }

      if(behaviorType != -1) {
        if ("IOS".equals(platform)) {
          if (behaviorType > 0) {
            "1"
          }
          else {
            "0"
          }
        }
        else {
          if (behaviorType == 0) {
            "1"
          }
          else {
            "0"
          }
        }
      }
      else{
        "0"
      }
    }
    val getBehaviorFunc = udf(getBehavior)
    val exposureRecord2 = exposureRecord.withColumn("isNagi", getBehaviorFunc(col("record_platform"), col("record_ext_info")))

    val nagiRecord = exposureRecord2.filter(exposureRecord2("isNagi").equalTo(1))
    val posiRecord = userXrecord.filter(userXrecord("record_sop_type").equalTo("0X80066FA")).select("uin", "active_level", "record_date_key", "record_sop_type", "record_article_id", "record_gender", "record_age", "record_ext_info", "record_city_id", "record_uin")

    val posiRecordTmp = posiRecord.select("uin", "record_article_id")
      .toDF("uin2", "record_article_id2")

    val nagiRecord2 = nagiRecord.join(posiRecordTmp, nagiRecord("uin") === posiRecordTmp("uin2") && nagiRecord("record_article_id") === posiRecordTmp("record_article_id2"), "left_outer")
    val nagiRecord3 = nagiRecord2.filter(" uin2 is null ").select("uin", "active_level", "record_date_key", "record_sop_type", "record_article_id", "record_gender", "record_age", "record_ext_info", "record_city_id", "record_uin")

    //val Array(leftNagiData, ignoreData) = nagiRecord3.randomSplit(Array(0.6, 0.4))

    val recordNew = posiRecord.union(nagiRecord3)

    val setLabel: (String => Long) = (arg: String) => {
      if ("0X80066FA".equals(arg)) {
        1
      }
      else {
        0
      }
    }
    val labelFunc = udf(setLabel)
    val record3 = recordNew.withColumn("label", labelFunc(col("record_sop_type")))

    /////////////////////////==================================================================

    val cutHour: (Long => String) = (arg: Long) => {
      try {
        arg.toString.substring(8)
      }
      catch {
        case ex: Exception => "-1"
      }
    }
    val hourFunc = udf(cutHour)
    val userXrecord2 = record3.withColumn("hour", hourFunc(col("record_date_key")))

    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin","hour","algorithm_id"
    val getAlgorithmId: (String => String) = (arg: String) => {
      try {
        val json = parse(arg).asInstanceOf[JObject]
        json.values("algorithm_id").toString
      }
      catch {
        case ex: Exception => "-1"
      }

    }
    val algorithmFunc = udf(getAlgorithmId)
    val userXrecord3 = userXrecord2.withColumn("algorithm_id", algorithmFunc(col("record_ext_info"))).select("uin", "active_level", "record_date_key", "record_sop_type", "record_article_id", "record_gender", "record_age", "record_city_id", "record_uin", "hour", "algorithm_id", "label")


    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin","hour","algorithm_id","age_stage"
    val getAgeStage: (Long => String) = (arg: Long) => {
      if (arg >= 1 && arg <= 11) {
        "1-11"
      }
      else if (arg >= 12 && arg <= 15) {
        "12-15"
      }
      else if (arg >= 16 && arg <= 18) {
        "16-18"
      }
      else if (arg >= 19 && arg <= 23) {
        "19-23"
      }
      else if (arg >= 24 && arg <= 30) {
        "24-30"
      }
      else if (arg >= 31 && arg <= 40) {
        "31-40"
      }
      else if (arg >= 41 && arg <= 50) {
        "41-50"
      }
      else {
        ""
      }
    }
    val ageStageFunc = udf(getAgeStage)
    val userXrecord4 = userXrecord3.withColumn("age_stage", ageStageFunc(col("record_age")))

    //    val infoTable = tdw2.table("im_data_02_04_091_monthly", Seq(newDateStr))
    //    val info = infoTable.select("uin", "mobile_login_count").toDF("info_uin", "info_login_cnt")
    //
    //    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    //    // "record_ext_info","record_city_id","record_uin","info_uin","hour","algorithm_id","info_login_cnt","age_stage"
    //    val userXrecordXinfo = userXrecord4.join(info, userXrecord4("record_uin") === info("info_uin"))


    //==============================================================
    //algorithm rate
    val algorithmRateTable = tdw.table("huangyao_article_algorithm_transrate", Seq("p_" + dateStr))
    val algorithmRate = algorithmRateTable.select("gender", "age", "algorithm_id", "rate").toDF("ar_gender", "ar_age", "ar_algorithm_id", "ar_rate")

    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin","info_uin","hour","algorithm_id","info_login_cnt","age_stage","ar_rate"
    val XalgorithmRate = userXrecord4.join(algorithmRate, userXrecord4("record_gender") === algorithmRate("ar_gender")
      && userXrecord4("algorithm_id") === algorithmRate("ar_algorithm_id") && userXrecord4("age_stage") === algorithmRate("ar_age"))


    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin","info_uin","hour","algorithm_id","info_login_cnt","age_stage",
    // "ar_rate","titlelength","coverpicnum","qualityscore","accountscore","titlecontainellipses",
    // account_read_score,account_click_score,account_like_score,account_comment_score,account_health_score,account_res_score
    val articleTable = tdw.table("huangyao_article_featurein3days", DateUtil.getThreeDate(dateStr)) //////////////////////////////////////////
    val Xarticle = XalgorithmRate.join(articleTable, XalgorithmRate("record_article_id") === articleTable("articleid"))

    //=============================================================
    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin","info_uin","hour","algorithm_id","info_login_cnt","age_stage",
    // "ar_rate","titlelength","coverpicnum","qualityscore","accountscore","tag_id","titlecontainellipses"
    val articleTag = tdw.table("simsonhe_kandian_article_tag", Seq("p_" + dateStr))
    val articleTag2 = articleTag.filter(articleTag("tag_level").equalTo(16))
    val XarticleTag = Xarticle.join(articleTag2, Xarticle("record_article_id") === articleTag2("cms_article_id"))

    //=============================================================
    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin","info_uin","hour","algorithm_id","info_login_cnt","age_stage",
    // "ar_rate","titlelength","coverpicnum","qualityscore","accountscore","tag_id","citylevel","titlecontainellipses"
    val cityInfo = tdw.table("simsonhe_kandian_city_info")
    val XcityLevel = XarticleTag.join(cityInfo, XarticleTag("record_city_id") === cityInfo("cityid"))

    //=============================================================
    val clickRateTable = tdw.table("spenceryang_feeds_click_rate", Seq("p_" + dateStr)).select("age", "gender", "first_class_id", "rate")
      .toDF("click_age", "click_gender", "click_first_class_id", "channel_click_rate")
    val XclickRate = XcityLevel.join(clickRateTable, XcityLevel("age_stage") === clickRateTable("click_age")
      && XcityLevel("record_gender") === clickRateTable("click_gender") && XcityLevel("tag_id") === clickRateTable("click_first_class_id")) //.cache()

    //XclickRate.select("click_age","click_gender","click_first_class_id","channel_click_rate").show(50)

    val commentRateTable = tdw.table("spenceryang_feeds_comment_rate", Seq("p_" + dateStr)).select("age", "gender", "first_class_id", "rate")
      .toDF("comment_age", "comment_gender", "comment_first_class_id", "channel_comment_rate")
    val XcommentRate = XclickRate.join(commentRateTable, XclickRate("age_stage") === commentRateTable("comment_age")
      && XclickRate("record_gender") === commentRateTable("comment_gender") && XclickRate("tag_id") === commentRateTable("comment_first_class_id")) //.cache()

    //XcommentRate.select("comment_age","comment_gender","comment_first_class_id","channel_comment_rate").show(50)

    val likeRateTable = tdw.table("spenceryang_feeds_like_rate", Seq("p_" + dateStr)).select("age", "gender", "first_class_id", "rate")
      .toDF("like_age", "like_gender", "like_first_class_id", "channel_like_rate")
    val XlikeRate = XcommentRate.join(likeRateTable, XcommentRate("age_stage") === likeRateTable("like_age")
      && XcommentRate("record_gender") === likeRateTable("like_gender") && XcommentRate("tag_id") === likeRateTable("like_first_class_id")) //.cache()

    //XlikeRate.select("like_age","like_gender","like_first_class_id","channel_like_rate").show(50)

    //==========================================================================
    val downPageRatio = tdw.table("huangyao_article_downpageratio", Seq("p_" + dateStr)).select("inneruniqueid", "downpageratio").toDF("ratio_inneruniqueid", "ratio_downpageratio")
    val Xratio = XlikeRate.join(downPageRatio, XlikeRate("inneruniqueid") === downPageRatio("ratio_inneruniqueid"), "left_outer")

    val readCompleteTran: (String => String) = (arg: String) => {
      if (arg == null) {
        "0.0"                          //"0.7622"
      }
      else {
        arg
      }

    }
    val readCompleteTranFunc = udf(readCompleteTran)
    val XratioTran = Xratio.withColumn("read_complete", readCompleteTranFunc(col("ratio_downpageratio")))

    //===================================
    val stayTime = tdw.table("huangyao_article_staytime", Seq("p_" + dateStr)).select("inneruniqueid", "staytime").toDF("stay_inneruniqueid", "stay_staytime")
    val XstayTime = XratioTran.join(stayTime, XlikeRate("inneruniqueid") === stayTime("stay_inneruniqueid"), "left_outer")

    val stayTimeTran: (String => String) = (arg: String) => {
      if (arg == null) {
         "0.0"             /////0.2947 //avg time
      }
      else {
        arg
      }

    }
    val stayTimeTranFunc = udf(stayTimeTran)
    val XstayTimeTran = XstayTime.withColumn("read_time", stayTimeTranFunc(col("stay_staytime")))



    var isWeekend = 0
    if (dayIndex == 1 || dayIndex == 7) {
      isWeekend = 1
    }

    val XarticleTag2 = XstayTimeTran.withColumn("click_prefer", lit(0)).withColumn("stay_time", lit(0)).withColumn("attract_point", lit(0))
      .withColumn("read_time", lit(0)).withColumn("title_emotion", lit(0)).withColumn("title_attract", lit(0))
      .withColumn("final_date_key", lit(dateStr)).withColumn("info_login_cnt", lit(0)).withColumn("isHoliday", lit(isWeekend))
    //新增------------------------------------------------------------------------------

    //"uin","active_level","record_date_key","record_sop_type", "record_article_id", "record_gender", "record_age",
    // "record_ext_info","record_city_id","record_uin","info_uin","hour","algorithm_id","info_login_cnt","age_stage",
    // "ar_rate","titlelength","coverpicnum","qualityscore","accountscore","tag_id","citylevel","titlecontainellipses"

    val accountScoreTran: (String => String) = (arg: String) => {
      if (arg == null) {
        "0.0"
      }
      else {
        arg
      }

    }
    val accountScoreTranFunc = udf(accountScoreTran)
    val XaccountScoreTran = XarticleTag2.withColumn("accountscore_new", accountScoreTranFunc(col("accountscore")))

    val reportTable = tdw2.table("kd_dsl_recomm_score_online_v2_fht0", DateUtil.getOneDayIinterval(dateStr))
    val reportData = reportTable.select("tdbank_imp_date","uin","diid","usdeviceinfo","didocscore","diusermodelscore","dihot","ditagscore","dihqtagscore").toDF("report_tdbank_imp_date","report_uin","diid","usdeviceinfo","didocscore","diusermodelscore","dihot","ditagscore","dihqtagscore")

    val Xreport = XaccountScoreTran.join(reportData,XaccountScoreTran("uin")===reportData("report_uin") && XaccountScoreTran("inneruniqueid")===reportData("diid"))

    val usermodelScoreTran: (String => String) = (arg: String) => {
      if (arg == null) {
        "0.0"
      }
      else {
        arg
      }

    }
    val usermodelScoreTranFunc = udf(usermodelScoreTran)
    val XusermodelScoreTran = Xreport.withColumn("diusermodelscore_trans", usermodelScoreTranFunc(col("diusermodelscore")))

    val dihotTran: (String => String) = (arg: String) => {
      if (arg == null) {
        "0.0"
      }
      else {
        arg
      }

    }
    val dihotTranFunc = udf(dihotTran)
    val XdihotTran = XusermodelScoreTran.withColumn("dihot_trans", dihotTranFunc(col("dihot")))

    val ditagscoreTran: (String => String) = (arg: String) => {
      if (arg == null) {
        "0.0"
      }
      else {
        arg
      }

    }
    val ditagscoreTranFunc = udf(ditagscoreTran)
    val XditagscoreTra = XdihotTran.withColumn("ditagscore_trans", ditagscoreTranFunc(col("ditagscore")))

    val dihqtagscoreTran: (String => String) = (arg: String) => {
      if (arg == null) {
        "0.0"
      }
      else {
        arg
      }

    }
    val dihqtagscoreTranFunc = udf(dihqtagscoreTran)
    val finalData = XditagscoreTra.withColumn("dihqtagscore_trans", dihqtagscoreTranFunc(col("dihqtagscore")))


    val finalData2 = finalData.filter(
      finalData("final_date_key") >= 0 &&
        finalData("hour") >= 0 &&
        finalData("record_gender") >= 0 &&
        finalData("record_age") >= 0 &&
        finalData("citylevel") >= 0 &&
        finalData("info_login_cnt") >= 0 &&
        finalData("active_level") >= 0 &&
        finalData("click_prefer") >= 0 &&
        finalData("stay_time") >= 0 &&
        finalData("titlelength") >= 0 &&
        finalData("coverpicnum") >= 0 &&
        finalData("qualityscore") >= 0 &&
        finalData("accountscore_new") >= 0 &&
        finalData("attract_point") >= 0 &&
        finalData("read_time") >= 0 &&
        finalData("read_complete") >= 0 &&
        finalData("title_emotion") >= 0 &&
        finalData("title_attract") >= 0 &&
        finalData("channel_click_rate") >= 0 &&
        finalData("channel_like_rate") >= 0 &&
        finalData("channel_comment_rate") >= 0 &&
        finalData("tag_id") >= 0 &&
        finalData("ar_rate") >= 0 &&
        finalData("algorithm_id") >= 0 &&
        finalData("uin") >= 0 &&
        finalData("record_article_id") >= 0 &&
        finalData("didocscore") >= 0 &&
        finalData("diusermodelscore_trans") >= 0 &&
        finalData("dihot_trans") >= 0 &&
        finalData("ditagscore_trans") >= 0 &&
        //"usdeviceinfo"有字符
        finalData("dihqtagscore_trans") >= 0 &&
        finalData("account_read_score") >= 0 &&
        finalData("account_click_score") >= 0 &&
        finalData("account_like_score") >= 0 &&
        finalData("account_comment_score") >= 0 &&
        finalData("account_health_score") >= 0 &&
        finalData("account_res_score") >= 0
    )

    //XarticleTag3.show(500)

//    di_score DOUBLE,
//    di_usermodel_score DOUBLE,
//    di_hot DOUBLE,
//    di_tag_score DOUBLE,
//    network STRING


    tdw.saveToTable(finalData2.select("final_date_key", "hour", "record_gender", "record_age", "citylevel",
      "info_login_cnt", "active_level", "click_prefer", "stay_time", "titlelength", "coverpicnum", "qualityscore", "accountscore_new"
      , "attract_point", "read_time", "read_complete", "title_emotion", "title_attract", "channel_click_rate",
      "channel_like_rate", "channel_comment_rate", "tag_id", "ar_rate", "algorithm_id", "label", "uin", "record_article_id", "isHoliday",
      "titlecontainellipses","didocscore","diusermodelscore_trans","dihot_trans","ditagscore_trans","usdeviceinfo","dihqtagscore_trans",
      "account_read_score","account_click_score","account_like_score","account_comment_score","account_health_score","account_res_score"
    ).repartition(500), "simsonhe_kandian_train_data", "p_" + dateStr, null, false)

  }
}