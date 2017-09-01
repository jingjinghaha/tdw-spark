package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil, TableDesc}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by franniewu on 2017/5/19.
  */
object Statistics1stChannel {

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

    // the 1st channel distribution of all users
    val n10 = dataTdw.count()
    println(n10)
    val avg10 = dataTdw.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg10.show()
    val avg10_new = avg10.withColumn("no",lit(10)).withColumn("num",lit(n10))

    // the 1st channel distribution of meizhuang users
    val group20 = dataTdw.filter(dataTdw("id5").gt(0))
    val n20 = group20.count()
    println(n20)
    val avg20 = group20.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg20.show()
    val avg20_new = avg20.withColumn("no",lit(20)).withColumn("num",lit(n20))

    // the 1st channel distribution of non-meizhuang users
    val group0 = dataTdw.filter(dataTdw("id5").equalTo(0))
    val n0 = group0.count()
    println(n0)
    val avg0 = group0.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg0.show()
    val avg0_new = avg0.withColumn("no",lit(0)).withColumn("num",lit(n0))

    val group1 = dataTdw.filter(dataTdw("id5").gt(0) && dataTdw("id5").lt(0.1))
    val n1 = group1.count()
    println(n1)
    val avg1 = group1.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg1.show()
    val avg1_new = avg1.withColumn("no",lit(1)).withColumn("num",lit(n1))

    val group2 = dataTdw.filter(dataTdw("id5").gt(0.1) && dataTdw("id5").lt(0.2))
    val n2 = group2.count()
    println(n2)
    val avg2 = group2.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg2.show()
    val avg2_new = avg2.withColumn("no",lit(2)).withColumn("num",lit(n2))

    val group3 = dataTdw.filter(dataTdw("id5").gt(0.2) && dataTdw("id5").lt(0.3))
    val n3 = group3.count()
    println(n3)
    val avg3 = group3.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg3.show()
    val avg3_new = avg3.withColumn("no",lit(3)).withColumn("num",lit(n3))

    val group4 = dataTdw.filter(dataTdw("id5").gt(0.3) && dataTdw("id5").lt(0.4))
    val n4 = group4.count()
    println(n4)
    val avg4 = group4.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg4.show()
    val avg4_new = avg4.withColumn("no",lit(4)).withColumn("num",lit(n4))

    val group5 = dataTdw.filter(dataTdw("id5").gt(0.4) && dataTdw("id5").lt(0.5))
    val n5 = group5.count()
    println(n5)
    val avg5 = group5.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg5.show()
    val avg5_new = avg5.withColumn("no",lit(5)).withColumn("num",lit(n5))

    val group6 = dataTdw.filter(dataTdw("id5").gt(0.5))
    val n6 = group6.count()
    println(n6)
    val avg6 = group6.select(mean("id0"),mean("id1"),mean("id2"),mean("id3"),mean("id4"),mean("id5"),
      mean("id6"),mean("id7"),mean("id8"),mean("id9"),mean("id10"),mean("id11"),mean("id12"),mean("id13"),
      mean("id14"),mean("id15"),mean("id16"),mean("id17"),mean("id18"),mean("id19"),mean("id20"),mean("id21"),
      mean("id22"),mean("id23"),mean("id24"),mean("id25"),mean("id26"),mean("id27"),mean("id28"),mean("id29"),
      mean("id30"),mean("id31"),mean("id32"),mean("id33"),mean("id34"),mean("id35"),mean("id36"),mean("id37"),
      mean("id38"),mean("id39"),mean("id40"),mean("id41"),mean("id42"),mean("id43"),mean("id44"),mean("id45"),mean("id46"))
    avg6.show()
    val avg6_new = avg6.withColumn("no",lit(6)).withColumn("num",lit(n6))

    val avg = avg20_new.union(avg10_new).union(avg0_new).union(avg1_new).union(avg2_new).union(avg3_new)
      .union(avg4_new).union(avg5_new).union(avg6_new)
      .toDF("id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46", "no", "num")
    avg.show()

    val output = avg.withColumn("date_key",lit(dateStr))
    createTable("franniewu_meizhuang_1st_channel_avg",user,pwd,dateStr)
    tdw.saveToTable(output.select("date_key","no","num","id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9",
      "id10", "id11", "id12", "id13", "id14", "id15", "id16", "id17", "id18", "id19",
      "id20", "id21", "id22", "id23", "id24", "id25", "id26", "id27", "id28", "id29",
      "id30", "id31", "id32", "id33", "id34", "id35", "id36", "id37", "id38", "id39",
      "id40", "id41", "id42", "id43", "id44", "id45", "id46"),"franniewu_meizhuang_1st_channel_avg","p_"+dateStr)

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
        Array("id24","Double","first channel weight"),
        Array("id25","Double","first channel weight"),
        Array("id26","Double","first channel weight"),
        Array("id27","Double","first channel weight"),
        Array("id28","Double","first channel weight"),
        Array("id29","Double","first channel weight"),
        Array("id30","Double","first channel weight"),
        Array("id31","Double","first channel weight"),
        Array("id32","Double","first channel weight"),
        Array("id33","Double","first channel weight"),
        Array("id34","Double","first channel weight"),
        Array("id35","Double","first channel weight"),
        Array("id36","Double","first channel weight"),
        Array("id37","Double","first channel weight"),
        Array("id38","Double","first channel weight"),
        Array("id39","Double","first channel weight"),
        Array("id40","Double","first channel weight"),
        Array("id41","Double","first channel weight"),
        Array("id42","Double","first channel weight"),
        Array("id43","Double","first channel weight"),
        Array("id44","Double","first channel weight"),
        Array("id45","Double","first channel weight"),
        Array("id46","Double","first channel weight")
      ))
      .setComment("this is a table to record average 1st channel weight of meizhuang segments")
      .setCompress(true)
      .setFileFormat("RCFILE")
      .setPartType("LIST")
      .setPartField("date_key")
    tdwUtil.createTable(tableDesc)
    tdwUtil.createListPartition(tableName, "p_"+dateStr, dateStr)
  }
}
