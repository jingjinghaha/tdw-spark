package com.tencent.tdw.spark.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object RecordJoinInfo {

  def main(args: Array[String]) {

    val cmdArgs = new GeneralArgParser(args)
    val dateStr:String = cmdArgs.getStringValue("dateStr")
    val user:String = cmdArgs.getStringValue("user")
    val pwd:String = cmdArgs.getStringValue("pwd")
    var partStr:String =""

    //im_data_01_03_025_split1_hour分区
    var dateSeq:Seq[String] = Seq();

    for (timeH <- 0 to 23) {
      if (timeH < 10) {
        dateSeq = dateSeq.:+ ("p_" + dateStr + "0" + timeH)
        partStr = partStr+dateStr + "0" + timeH+","
      }
      else {
        dateSeq = dateSeq.:+ ("p_" + dateStr + timeH)
        if(timeH==23){
          partStr = partStr+dateStr +  timeH
        }
        else{
          partStr = partStr+dateStr +  timeH+","
        }
      }
    }

    //info分区 取上个月的数据
    val df=new SimpleDateFormat("yyyyMMdd")
    val dfs=new SimpleDateFormat("yyyyMM")

    val date = df.parse(dateStr)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_MONTH, -10)
    calendar.add(Calendar.MONTH, -1)

    val newDate = calendar.getTime
    val newDateStr = "p_"+dfs.format(newDate)

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "imdataoss")

    val recordTable = tdw.table("im_data_01_03_025_split1_hour",dateSeq)
    val infoTable = tdw.table("im_data_02_04_091_monthly",Seq(newDateStr))

    val info1 = infoTable.select("uin","gender","age","mobile_login_count","most_login_city").toDF("info_uin","info_gender","info_age","info_login_cnt","info_login_city")
    val info2 = info1.filter(info1("info_gender").gt(0) && info1("info_age").gt(0) && info1("info_age").lt(51) && info1("info_login_cnt").gt(0))

    val record1 = recordTable.select("date_key","dwfuin","sop_type","dwflag3","sflag4","sflag5","platform_name").toDF("record_date_key","record_uin","record_sop_type","record_article_id","record_strategy_id","record_ext","record_platform_name")
    val record2 = record1.filter(record1("record_sop_type").equalTo("0X80066F3") || record1("record_sop_type").equalTo("0X80066F4") ||record1("record_sop_type").equalTo("0X80066FA")||record1("record_sop_type").equalTo("0X80066FC")||record1("record_sop_type").equalTo("0X8007626"))

    val record3 = record2.join(info2,info2("info_uin")===record2("record_uin"));

    //val tmp = record3.filter(record3("info_age").lt(18)).count();

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
    val record4 = record3.withColumn("age_stage", ageStageFunc(col("info_age")))


    //"age_stage","count"
//    val record5 = record4.groupBy(record4("age_stage")).count()
//
//    record5.show(50)

    val tdw2 = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    //新增------------------------------------------------------------------------------
    tdw2.saveToTable(record4.select("record_date_key", "record_uin", "record_sop_type","record_strategy_id","record_ext",
      "info_gender","info_age","record_article_id","info_login_city","age_stage","record_platform_name").repartition(50), "simsonhe_kandian_split_hour_info","p_"+dateStr,null,false)

  }
}