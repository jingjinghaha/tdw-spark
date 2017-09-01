package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

/**
  * Created by franniewu on 2017/5/11.
  */
object GetUniqUin {

  val tableName = "franniewu_unique_uin"
  // val tableName = "franniewu_unique_uin_meizhuang"
  //  val tableName = "franniewu_unique_uin_comic"
  //  val tableName = "franniewu_unique_uin_tech"
  //  val tableName = "franniewu_unique_uin_sport"
  //  val tableName = "franniewu_unique_uin_game"
  //  val tableName = "franniewu_unique_uin_ent"

  def main(args: Array[String]): Unit ={
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")


    //    val dateSegment: List[String] = List("20170411","20170412","20170413","20170414","20170415",
    //      "20170416","20170417","20170418","20170419","20170420","20170421","20170422","20170423","20170424",
    //      "20170425","20170426","20170427","20170428","20170429","20170430","20170501","20170502","20170503",
    //      "20170504","20170505","20170506","20170507","20170508","20170509")

    //    val dataTdw0 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170410"))
    //    val output0 = dataTdw0.select("uin")
    //    println(output0.count())
    //
    //    val dataTdw1 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170411"))
    //    val dailyUserInfo1 = dataTdw1.select("uin")
    //    val output1 = output0.join(dailyUserInfo1,Seq("uin"),joinType = "outer")
    //    println(output1.count())
    //
    //    val dataTdw2 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170412"))
    //    val dailyUserInfo2 = dataTdw2.select("uin")
    //    val output2 = output1.join(dailyUserInfo2,Seq("uin"),joinType = "outer")
    //    println(output2.count())
    //
    //    val dataTdw3 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170413"))
    //    val dailyUserInfo3 = dataTdw3.select("uin")
    //    val output3 = output2.join(dailyUserInfo3,Seq("uin"),joinType = "outer")
    //    println(output3.count())
    //
    //    val dataTdw4 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170414"))
    //    val dailyUserInfo4 = dataTdw4.select("uin")
    //    val output4 = output3.join(dailyUserInfo4,Seq("uin"),joinType = "outer")
    //    println(output4.count())
    //
    //    val dataTdw5 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170415"))
    //    val dailyUserInfo5 = dataTdw5.select("uin")
    //    val output5 = output4.join(dailyUserInfo5,Seq("uin"),joinType = "outer")
    //    println(output5.count())
    //
    //    val dataTdw6 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170416"))
    //    val dailyUserInfo6 = dataTdw6.select("uin")
    //    val output6 = output5.join(dailyUserInfo6,Seq("uin"),joinType = "outer")
    //    println(output6.count())
    //
    //    val dataTdw7 = tdw.table("haohaochen_user_potrait_tag", Seq("p_20170417"))
    //    val dailyUserInfo7 = dataTdw7.select("uin")
    //    val output7 = output6.join(dailyUserInfo7,Seq("uin"),joinType = "outer")
    //    println(output7.count())

    val spark = SparkSession.builder().getOrCreate()
    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val dataTdw = tdw.table("haohaochen_user_potrait_tag")

    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")

    val no_vec: List[String] = List("0,id5", "1,id33", "2,id2", "3,id1", "4,id10", "5,id3")

    for (items <- no_vec){
      val itemSplit = items.split(",")
      val no = itemSplit.apply(0)
      val col = itemSplit.apply(1)
      tdwUtil.dropPartition(tableName,"p_"+no)
      tdwUtil.createListPartition(tableName,"p_"+no,no)

      val output = dataTdw.filter(dataTdw(col).gt(0)).select("uin")
      println(output.count())

      val uniqueUin = output.dropDuplicates("uin").withColumn("no", lit(no))
      println(uniqueUin.count())

      tdw.saveToTable(uniqueUin.select("no","uin").repartition(500), tableName, "p_"+no, null, false)
    }

  }

}
