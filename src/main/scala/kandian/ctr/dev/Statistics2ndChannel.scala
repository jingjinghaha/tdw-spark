package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil, TableDesc}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by franniewu on 2017/5/19.
  */
object Statistics2ndChannel {

  def main(args: Array[String]): Unit ={

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()
    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val dataTdw = tdw.table("franniewu_1st_channel_weight", Seq("p_" + dateStr))
    val output0 = dataTdw.filter(dataTdw("id5").gt(0)).select("uin")
    println("the number of users have meizhuang weight is " + output0.count())

    val data2 = tdw.table("franniewu_2nd_channel_weight_women",Seq("p_"+dateStr))
      .toDF("date_key2","uin2","id0", "id1", "id2", "id3", "id4",
      "id5", "id6", "id7", "id8", "id9", "id10", "id11", "id12", "id13", "id14", "id15", "id16",
      "id17", "id18", "id19", "id20", "id21", "id22", "id23", "id24")

    // the 2nd channel distribution of meizhuang users
    val group20 = dataTdw.filter(dataTdw("id5").gt(0)).select("uin")
    val second_group20 = group20.join(data2, group20("uin") === data2("uin2"),"inner")
    val n20 = second_group20.count()
    val avg20 = second_group20.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg20.show()
    val avg20_new = avg20.withColumn("no",lit(20)).withColumn("num",lit(n20))

    // the 2nd channel distribution of none-meizhuang users
    val group0 = dataTdw.filter(dataTdw("id5").equalTo(0)).select("uin")
    val second_group0 = group0.join(data2, group0("uin") === data2("uin2"),"inner")
    val n0 = second_group0.count()
    val avg0 = second_group0.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg0.show()
    val avg0_new = avg0.withColumn("no",lit(0)).withColumn("num",lit(n0))

    val group1 = dataTdw.filter(dataTdw("id5").gt(0) && dataTdw("id5").lt(0.1)).select("uin")
    val second_group1 = group1.join(data2, group0("uin") === data2("uin2"),"inner")
    val n1 = second_group1.count()
    println(n1)
    val avg1 = second_group1.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg1.show()
    val avg1_new = avg1.withColumn("no",lit(1)).withColumn("num",lit(n1))

    val group2 = dataTdw.filter(dataTdw("id5").gt(0.1) && dataTdw("id5").lt(0.2)).select("uin")
    val second_group2 = group2.join(data2, group0("uin") === data2("uin2"),"inner")
    val n2 = second_group2.count()
    println(n2)
    val avg2 = second_group2.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg2.show()
    val avg2_new = avg2.withColumn("no",lit(2)).withColumn("num",lit(n2))

    val group3 = dataTdw.filter(dataTdw("id5").gt(0.2) && dataTdw("id5").lt(0.3)).select("uin")
    val second_group3 = group3.join(data2, group0("uin") === data2("uin2"),"inner")
    val n3 = second_group3.count()
    println(n3)
    val avg3 = second_group3.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg3.show()
    val avg3_new = avg3.withColumn("no",lit(3)).withColumn("num",lit(n3))

    val group4 = dataTdw.filter(dataTdw("id5").gt(0.3) && dataTdw("id5").lt(0.4)).select("uin")
    val second_group4 = group4.join(data2, group0("uin") === data2("uin2"),"inner")
    val n4 = second_group4.count()
    println(n4)
    val avg4 = second_group4.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg4.show()
    val avg4_new = avg4.withColumn("no",lit(4)).withColumn("num",lit(n4))

    val group5 = dataTdw.filter(dataTdw("id5").gt(0.4) && dataTdw("id5").lt(0.5)).select("uin")
    val second_group5 = group5.join(data2, group0("uin") === data2("uin2"),"inner")
    val n5 = second_group5.count()
    println(n5)
    val avg5 = second_group5.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg5.show()
    val avg5_new = avg5.withColumn("no",lit(5)).withColumn("num",lit(n5))

    val group6 = dataTdw.filter(dataTdw("id5").gt(0.5)).select("uin")
    val second_group6 = group6.join(data2, group0("uin") === data2("uin2"),"inner")
    val n6 = second_group6.count()
    println(n6)
    val avg6 = second_group6.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"))
    avg6.show()
    val avg6_new = avg6.withColumn("no",lit(6)).withColumn("num",lit(n6))

    val avg = avg20_new.union(avg0_new).union(avg1_new).union(avg2_new).union(avg3_new).union(avg4_new).union(avg5_new).union(avg6_new)
      .toDF("id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "no", "num")
    avg.show()

    val output = avg.withColumn("date_key",lit(dateStr))
    createTable("franniewu_meizhuang_2nd_channel_avg",user,pwd,dateStr)
    tdw.saveToTable(output.select("date_key","no","num","id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24"),"franniewu_meizhuang_2nd_channel_avg","p_"+dateStr)

  }

  def createTable(tableName:String,user:String,pwd:String,dateStr:String): Unit ={
    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    val tableDesc = new TableDesc().setTblName(tableName)
      .setCols(Seq(
        Array("date_key","String","the date"),
        Array("no","Int","the user segment"),
        Array("num","BigInt","the number if user in this segment"),
        Array("id0","Double","first channel weight"),
        Array("id1","Double","first channel weight"),
        Array("id2","Double","first channel weight"),
        Array("id3","Double","first channel weight"),
        Array("id4","Double","first channel weight"),
        Array("id5","Double","first channel weight"),
        Array("id6","Double","first channel weight"),
        Array("id7","Double","first channel weight"),
        Array("id8","Double","first channel weight"),
        Array("id9","Double","first channel weight"),
        Array("id10","Double","first channel weight"),
        Array("id11","Double","first channel weight"),
        Array("id12","Double","first channel weight"),
        Array("id13","Double","first channel weight"),
        Array("id14","Double","first channel weight"),
        Array("id15","Double","first channel weight"),
        Array("id16","Double","first channel weight"),
        Array("id17","Double","first channel weight"),
        Array("id18","Double","first channel weight"),
        Array("id19","Double","first channel weight"),
        Array("id20","Double","first channel weight"),
        Array("id21","Double","first channel weight"),
        Array("id22","Double","first channel weight"),
        Array("id23","Double","first channel weight"),
        Array("id24","Double","first channel weight")
      ))
      .setComment("this is a table to record average 2nd channel weight of meizhuang segments")
      .setCompress(true)
      .setFileFormat("RCFILE")
      .setPartType("LIST")
      .setPartField("date_key")
    tdwUtil.createTable(tableDesc)
    tdwUtil.createListPartition(tableName, "p_"+dateStr, dateStr)
  }
}
