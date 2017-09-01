package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, col}

object ArticleProfile {

  def main(args: Array[String]): Unit = {
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()
    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val articleProfile = tdw.table("haohaochen_article_channel", DateUtil.getOneWeek(dateStr))
    val filteredArticleProfile = articleProfile.filter(articleProfile("channel_2nd").gt(0))
    filteredArticleProfile.show()

    val firstChannel = tdw.table("haohaochen_channel_1st", Seq("p_"+dateStr)).select("inner_id","inner_eng_ame")
    val secondChannel = tdw.table("haohaochen_channel_2nd", Seq("p_"+dateStr)).select("inner_id","eng_name")

    //convert article 1st channel id to index
    val firstChannelSort = firstChannel.sort(firstChannel("inner_id").asc)
    firstChannelSort.show(100)
    val secondChannelSort = secondChannel.sort(secondChannel("inner_id").asc)
    secondChannelSort.show(600)

    var firstChannelMap: Map[Long, Int] = Map()
    val arr1 = firstChannelSort.collect()
    var index1 = 0
    for (channelId1 <- arr1) {
      println(channelId1.toString())
      firstChannelMap = firstChannelMap + (channelId1.getLong(0) -> index1)
      index1 = index1 + 1
    }
    val firstChannelVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(index1, Array(firstChannelMap.apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(index1, Array(), Array())
      }
    }
    val firstChannelFunc = udf(firstChannelVector)

    //convert article 2nd channel id to index
    var secondChannelMap: Map[Long, Int] = Map()
    val arr2 = secondChannelSort.collect()
    var index2 = 0
    for (channelId2 <- arr2) {
      println(channelId2.toString())
      secondChannelMap = secondChannelMap + (channelId2.getLong(0) -> index2)
      index2 = index2 + 1
    }
    val secondChannelVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(index2, Array(secondChannelMap.apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(index2, Array(), Array())
      }
    }
    val secondChannelFunc = udf(secondChannelVector)


    val df = filteredArticleProfile
      .withColumn("1st_vec", firstChannelFunc(col("channel_1st")))
      .withColumn("2nd_vec", secondChannelFunc(col("channel_2nd")))


    df.show()
  }
}
