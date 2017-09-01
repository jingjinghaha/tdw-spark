package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.{TDWUtil, TableDesc}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by franniewu on 2017/5/11.
  */
object GenerateTable {

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val tdwUtil = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    val tableDesc = new TableDesc().setTblName("franniewu_training_result")
      .setCols(
        Seq(Array("no", "int", "identify different algorithms"),
          Array("uin", "bigint", "this is uin"),
          Array()
        ))
      .setComment("this is a table to extract uin for 6 vertical categories")
      .setCompress(true)
      .setFileFormat("RCFILE")
      .setPartType("LIST")
      .setPartField("no")

//    val tableDesc = new TableDesc().setTblName("franniewu_1st_channel_clustering")
//      .setCols(Seq(
//        Array("date_key","String","the date of the data"),
//        Array("uin","BigInt","the user id"),
//        Array("label","int","the clustering label"),
//        Array("id0","Double","first channel weight"),
//        Array("id1","Double","first channel weight"),
//        Array("id2","Double","first channel weight"),
//        Array("id3","Double","first channel weight"),
//        Array("id4","Double","first channel weight"),
//        Array("id5","Double","first channel weight"),
//        Array("id6","Double","first channel weight"),
//        Array("id7","Double","first channel weight"),
//        Array("id8","Double","first channel weight"),
//        Array("id9","Double","first channel weight"),
//        Array("id10","Double","first channel weight"),
//        Array("id11","Double","first channel weight"),
//        Array("id12","Double","first channel weight"),
//        Array("id13","Double","first channel weight"),
//        Array("id14","Double","first channel weight"),
//        Array("id15","Double","first channel weight"),
//        Array("id16","Double","first channel weight"),
//        Array("id17","Double","first channel weight"),
//        Array("id18","Double","first channel weight"),
//        Array("id19","Double","first channel weight"),
//        Array("id20","Double","first channel weight"),
//        Array("id21","Double","first channel weight"),
//        Array("id22","Double","first channel weight"),
//        Array("id23","Double","first channel weight"),
//        Array("id24","Double","first channel weight"),
//        Array("id25","Double","first channel weight"),
//        Array("id26","Double","first channel weight"),
//        Array("id27","Double","first channel weight"),
//        Array("id28","Double","first channel weight"),
//        Array("id29","Double","first channel weight"),
//        Array("id30","Double","first channel weight"),
//        Array("id31","Double","first channel weight"),
//        Array("id32","Double","first channel weight"),
//        Array("id33","Double","first channel weight"),
//        Array("id34","Double","first channel weight"),
//        Array("id35","Double","first channel weight"),
//        Array("id36","Double","first channel weight"),
//        Array("id37","Double","first channel weight"),
//        Array("id38","Double","first channel weight"),
//        Array("id39","Double","first channel weight"),
//        Array("id40","Double","first channel weight"),
//        Array("id41","Double","first channel weight"),
//        Array("id42","Double","first channel weight"),
//        Array("id43","Double","first channel weight"),
//        Array("id44","Double","first channel weight"),
//        Array("id45","Double","first channel weight"),
//        Array("id46","Double","first channel weight")
//      ))
//      .setComment("this is a table to record the clustering result")
//      .setCompress(true)
//      .setFileFormat("RCFILE")
//      .setPartType("LIST")
//      .setPartField("abc")



    tdwUtil.createTable(tableDesc)
//    tdwUtil.createListPartition(tableName, "p_0", "0")

    sc.stop()
  }
}
