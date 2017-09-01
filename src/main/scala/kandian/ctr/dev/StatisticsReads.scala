package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.sql.SparkSession

/**
  * Created by franniewu on 2017/5/14.
  */
object StatisticsReads {
  def main(args: Array[String]): Unit ={
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()
    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val dataTdw = tdw.table("franniewu_1st_channel_weight", Seq("p_" + dateStr))
    val output = dataTdw.filter(dataTdw("id5").gt(0)).select("uin")
    println("the number of users have meizhuang weight is " + output.count())
    val data = tdw.table("simsonhe_kandian_train_data", Seq("p_" + dateStr))
    val data_label = data.filter(data("label").equalTo(0))
    println("the number of users from train data is " + data.count())

    val userFeatures = output.join(data_label,data_label("uin") === output("uin"), "inner")
    println("the number of clicks from meizhuang users in " + dateStr + " is " + userFeatures.count())
    userFeatures.show(5)

    val df_men = userFeatures.filter(userFeatures("gender").equalTo(1))
    println("the number of male users in " + dateStr + " is " + df_men.count())

    val df_women = userFeatures.filter(userFeatures("gender").equalTo(2))
    println("the number of female users in " + dateStr + " is " + df_women.count())

    val ageSegment: List[String] = List("0,12,1-11","11,16,12-15","15,19,16-18","18,24,19-23","23,31,24-30",
      "30,41,31-40","40,51,41-50")

    for (item <- ageSegment) {
      val itemSplit = item.split(",")
      val df_age = userFeatures.filter(userFeatures("age").gt(itemSplit.apply(0).toLong) && userFeatures("age").lt(itemSplit.apply(1).toLong))
      println("the number of users in "+ dateStr+" of age "+itemSplit.apply(2)+" is " + df_age.count())
    }

    val citySegment: List[Int] = List(1,2,3,4,5)
    for (item <- citySegment){
      val df_city = userFeatures.filter(userFeatures("city_level").equalTo(item))
      println("the number of users in "+ dateStr+" from city level "+item.toString()+" is " + df_city.count())
    }

  }
}
