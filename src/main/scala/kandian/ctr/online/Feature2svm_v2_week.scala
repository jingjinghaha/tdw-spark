package com.tencent.kandian.ctr.online

import java.text.SimpleDateFormat
import java.util.Calendar

import com.tencent.kandian.ctr.dev.DateUtil
import com.tencent.kandian.ctr.online.DataProcessing._
import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

import scala.util.parsing.json.JSON

/**
  * Created by franniewu on 2017/7/20.
  */
object Feature2svm_v2_week {

  def main(args: Array[String]): Unit = {
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    val dateList: List[String] = List("20170720","20170721","20170722","20170723","20170724","20170725","20170726","20170727", "20170728", "20170729")
    //val dateList: List[String] = List("20170720")

    for (date <- dateList) {
      println(date)
      val data = tdw.table("rerank_train_data_kehuduan", Seq("p_" + date))

      val desiredNum = 230000000
      val dataNum = data.count()
      println("the number of data in "+date+" is: "+dataNum)
      val ratio = desiredNum.toDouble/dataNum.toDouble
      val Array(data_partition, data_left) = data.filter(
        data("date_key").isNotNull &&
          data("hour").isNotNull &&
          data("is_holiday").isNotNull &&
          data("label").isNotNull &&
          data("uin").isNotNull &&
          data("avtive_level").isNotNull &&
          data("gender").isNotNull &&
          data("age").isNotNull &&
          data("city_level").isNotNull &&
          data("user_read_time").isNotNull &&
          data("user_read_complete").isNotNull &&
          data("article_id").isNotNull &&
          data("titlelength").isNotNull &&
          data("coverpicnum").isNotNull &&
          data("quality_score").isNotNull &&
          data("account_score").isNotNull &&
          data("article_read_complete").isNotNull &&
          data("article_read_time").isNotNull &&
          data("comments").isNotNull &&
          data("zan").isNotNull &&
          data("share").isNotNull &&
          data("channel_click_rate").isNotNull &&
          data("channel_comment_rate").isNotNull &&
          data("channel_like_rate").isNotNull &&
          data("click_rate").isNotNull &&
          data("male_click_rate").isNotNull &&
          data("female_click_rate").isNotNull &&
          data("1_11_click_rate").isNotNull &&
          data("12_15_click_rate").isNotNull &&
          data("16_18_click_rate").isNotNull &&
          data("19_23_click_rate").isNotNull &&
          data("24_30_click_rate").isNotNull &&
          data("31_40_click_rate").isNotNull &&
          data("41_50_click_rate").isNotNull &&
          data("account_read_score").isNotNull &&
          data("account_click_score").isNotNull &&
          data("account_like_score").isNotNull &&
          data("account_comment_score").isNotNull &&
          data("account_health_score").isNotNull &&
          data("account_res_score").isNotNull).randomSplit(Array(ratio, 1-ratio))

      // get article profile and remove articles without second channel id
      val articleProfile = tdw.table("haohaochen_article_channel", DateUtil.getOneWeek(date)).toDF("today_date", "cms_article_id","channel_1st","channel_2nd","tag00","tag01","tag02","tag03","tag04","tag05","tag06","tag07","tag08","tag09","tag10","tag11","tag12","tag13","tag14","tag15","tag16","tag17","tag18","tag19")
      val filteredArticleProfile = articleProfile.filter(articleProfile("channel_2nd").gt(0))

      // get user profile from tdbank and remove duplicates
      val userProfileTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
      val userProfileData = userProfileTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + date))
        .select("tdbank_imp_date", "uin", "potrait").toDF("tdbank_imp_date", "uin2", "potrait")
      val userProfileDF = userProfileData.dropDuplicates("uin2").repartition(1000)

      // filter train data with user profile
      val filteredProfile = data_partition.join(userProfileDF, data_partition("uin") === userProfileDF("uin2"), "inner").repartition(1000)

      // filter train data with article profile
      val filteredArticle = filteredProfile.join(filteredArticleProfile, filteredArticleProfile("cms_article_id") === filteredProfile("article_id")).repartition(1000).cache()

      // the day of week, sparse to 7 dimensions
      val dayVector: (Long => Vector) = (dateString: Long) => {
        try {
          val df = new SimpleDateFormat("yyyyMMdd")
          val date = df.parse(dateString.toString)
          val calendar: Calendar = Calendar.getInstance
          calendar.setTime(date)
          val dayIndex = calendar.get(Calendar.DAY_OF_WEEK)
          Vectors.sparse(7, Array(dayIndex - 1), Array(1))
        }
        catch {
          case ex: Exception => Vectors.sparse(7, Array(), Array())
        }
      }
      val dayFunc = udf(dayVector)

      // get neisou first channel id map
      val firstChannel = tdw.table("haohaochen_channel_1st", Seq("p_"+dateStr)).select("inner_id","inner_eng_ame")
      val firstChannelSort = firstChannel.sort(firstChannel("inner_id").asc)
      //firstChannelSort.show(100)
      val firstChannelArr = firstChannelSort.collect()
      var firstChannelMap: Map[Long, Int] = Map()
      var firstChannelLen = 0
      for (channelId1 <- firstChannelArr) {
        //println(channelId1.toString())
        firstChannelMap = firstChannelMap + (channelId1.getLong(0) -> firstChannelLen)
        firstChannelLen = firstChannelLen + 1
      }
      val b1stChannelMap = spark.sparkContext.broadcast(firstChannelMap)
      val b1stChannelLen = spark.sparkContext.broadcast(firstChannelLen)

      //get neisou second channel id map
      val secondChannel = tdw.table("haohaochen_channel_2nd", Seq("p_"+dateStr)).select("inner_id","eng_name")
      val secondChannelSort = secondChannel.sort(secondChannel("inner_id").asc)
      //secondChannelSort.show(1000)
      val secondChannelArr = secondChannelSort.collect()
      var secondChannelMap: Map[Long, Int] = Map()
      var secondChannelLen = 0
      for (channelId2 <- secondChannelArr) {
        //println(channelId2.toString())
        secondChannelMap = secondChannelMap + (channelId2.getLong(0) -> secondChannelLen)
        secondChannelLen = secondChannelLen + 1
      }
      val b2ndChannelMap = spark.sparkContext.broadcast(secondChannelMap)
      val b2ndChannelLen = spark.sparkContext.broadcast(secondChannelLen)

      //convert user profile 1st channel id to vector
      val getUser1stChannelVector: (String => Vector) = (arg: String) => {
        try {
          val userTagList =
            for {
              Some(MapContainer(userMap)) <- List(JSON.parseFull(arg))
              ListContainer(tagList) = userMap("channel_model")
              MapContainer(tagMap) <- tagList
              DoubleContainer(tagIdx) = tagMap("id")
              DoubleContainer(tagWeight) = tagMap("weight")
              userTagIdxNew = b1stChannelMap.value.apply(tagIdx.toInt)
            } yield {
              (userTagIdxNew, tagWeight)
            }
          Vectors.sparse(b1stChannelLen.value, userTagList)
        } catch {
          case ex: Exception => {
            Vectors.sparse(b1stChannelLen.value, Array(), Array())
          }
        }
      }
      val getUser1stChannelFunc = udf(getUser1stChannelVector)

      // convert user profile 2nd channel id to vector
      val getUser2ndChannelVector: (String => Vector) = (arg: String) => {
        try {
          val userTagList =
            for {
              Some(MapContainer(userMap)) <- List(JSON.parseFull(arg))
              ListContainer(tagList) = userMap("channel_model_beta")
              MapContainer(tagMap) <- tagList
              DoubleContainer(tagIdx) = tagMap("id")
              DoubleContainer(tagWeight) = tagMap("weight")
              userTagIdxNew = b2ndChannelMap.value.apply(tagIdx.toInt)
            } yield {
              (userTagIdxNew, tagWeight)
            }
          Vectors.sparse(b2ndChannelLen.value, userTagList)
        } catch {
          case ex: Exception => Vectors.sparse(b2ndChannelLen.value, Array(), Array())
        }
      }
      val getUser2ndChannelFunc = udf(getUser2ndChannelVector)

      //convert article 1st channel id to vector
      val article1stChannelVector: (Long => Vector) = (arg: Long) => {
        try {
          Vectors.sparse(b1stChannelLen.value, Array(b1stChannelMap.value.apply(arg.toInt)), Array(1))
        }
        catch {
          case ex: Exception => Vectors.sparse(b1stChannelLen.value, Array(), Array())
        }
      }
      val article1stChannelFunc = udf(article1stChannelVector)

      //convert article 2nd channel id to vector
      val article2ndChannelVector: (Long => Vector) = (arg: Long) => {
        try {
          Vectors.sparse(b2ndChannelLen.value, Array(b2ndChannelMap.value.apply(arg.toInt)), Array(1))
        }
        catch {
          case ex: Exception => Vectors.sparse(b2ndChannelLen.value, Array(), Array())
        }
      }
      val article2ndChannelFunc = udf(article2ndChannelVector)

      val final_output = filteredArticle
        .withColumn("day_index_vec", dayFunc(col("date_key")))
        .withColumn("1stWeight", getUser1stChannelFunc(col("potrait")))
        .withColumn("2ndWeight", getUser2ndChannelFunc(col("potrait")))
        .withColumn("1st_vec", article1stChannelFunc(col("channel_1st")))
        .withColumn("2nd_vec", article2ndChannelFunc(col("channel_2nd")))

      // assemble features into one vector
      val assemblerOutput = new VectorAssembler()
        .setInputCols(Array(
          "day_index_vec",
          "hour",
          "is_holiday",
          "avtive_level",
          "gender",
          "age",
          "city_level",
          "user_read_time",
          "user_read_complete",
          "titlelength",
          "coverpicnum",
          "quality_score",
          "account_score",
          "article_read_complete",
          "article_read_time",
          "comments",
          "zan",
          "share",
          "channel_click_rate",
          "channel_comment_rate",
          "channel_like_rate",
          "click_rate",
          "male_click_rate",
          "female_click_rate",
          "1_11_click_rate",
          "12_15_click_rate",
          "16_18_click_rate",
          "19_23_click_rate",
          "24_30_click_rate",
          "31_40_click_rate",
          "41_50_click_rate",
          "account_read_score",
          "account_click_score",
          "account_like_score",
          "account_comment_score",
          "account_health_score",
          "account_res_score",
          "1stWeight", "2ndWeight", "1st_vec", "2nd_vec"
        ))
        .setOutputCol("features")
        .transform(final_output).select("label", "features").cache()

      // convert DataFrame columns to ML and random split the dataset
      val desiredDataNum = 100000000
      val finalDataNum = assemblerOutput.count()
      val ratio2 = desiredDataNum.toDouble/finalDataNum.toDouble
      val Array(train_data, test_data) = MLUtils.convertVectorColumnsToML(assemblerOutput).randomSplit(Array(ratio2, 1-ratio2))
      println(train_data.count(), test_data.count())

      // control the ratio between negative instances and positive instances
      val df_pos = train_data.filter(train_data("label").equalTo(1.0))
      val df_neg = train_data.filter(train_data("label").equalTo(0.0))
      val r = df_neg.count().toDouble/df_pos.count().toDouble
      println(r)
      val Array(partition1, partition2) = df_neg.randomSplit(Array(2.0/r, 1-(2.0/r)))
      val train_data_filter_neg = df_pos.union(partition1)

      // save final train data and test data to HDFS
      train_data_filter_neg.write.format("libsvm").mode("overwrite")
        .save("hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_train/feature_v5/sampled/" + date)
      test_data.write.format("libsvm").mode("overwrite")
        .save("hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_test/feature_v5/sampled/" + date)

      spark.sqlContext.clearCache()
    }
  }
}
