package com.tencent.tdw.spark.examples

import java.util.Properties

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by franniewu on 2017/5/3.
  */
object DataProcessing {

  def main(args: Array[String]): Unit = {
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    val data = tdw.table("simsonhe_kandian_train_data", DateUtil.getTodayDate(dateStr))
  }

  def transTrainData(originalData: DataFrame): DataFrame = {

    val newData = originalData.filter(originalData("avtive_level").gt(0.2) && originalData("gender").equalTo(2)
      && originalData("age").gt(15) && originalData("age").lt(19))

    //val newData = originalData.filter(originalData("avtive_level").gt(0.2))
    //val newData = originalData.filter(originalData("avtive_level").gt(10))

    val newData_posi = newData.filter(newData("label")===1)
    println("The number of positive instance: " + newData_posi.count())
    val newData_nagi = newData.filter(newData("label")===0)
    println("The number of negative instance: " + newData_nagi.count())

    //val Array(newData_left,newData_right) = newData.randomSplit(Array(0.1,0.9))
    //println("The number of instance after random split: " + newData_left.count())

    //val newData1 = newData_posi.union(newData_nagi_left)
    //println("The number of train instance: " + newData1.count())
    //newData1.printSchema()
    //newData1.show(5)

    val newData2 = DataProcessing.transFeature(newData)
    println(newData2.count())
    newData2.printSchema()
    newData2.show(5)

    newData2
  }


  def transFeature(originalData: DataFrame): DataFrame = {

    //hour, sparse to 24 dimensions
    val hourVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(24, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(24, Array(), Array())
      }
    }
    val hourFunc = udf(hourVector)
    val vHour = originalData.withColumn("hour_vector", hourFunc(col("hour")))
    vHour.show(5)

    val indexer = new StringIndexer()
      .setInputCol("hour")
      .setOutputCol("hour_index")
      .fit(originalData)
    val indexed = indexer.transform(originalData)
    val encoder = new OneHotEncoder()
      .setInputCol("hour_index")
      .setOutputCol("hour_vec")
    val encoded = encoder.transform(indexed)
    encoded.show(5)


    //gender, sparse to 3 dimensions
    val genderVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(3, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(3, Array(), Array())
      }
    }
    val genderFunc = udf(genderVector)
    val vGender = vHour.withColumn("gender_vector", genderFunc(col("gender")))
    vGender.show(5)

    //cityLevel
    val cityVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(6, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(6, Array(), Array())
      }
    }
    val cityFunc = udf(cityVector)
    val vCity = vGender.withColumn("city_level_vector", cityFunc(col("city_level")))
    vCity.show(5)

    //algorithm
    val algorithmToIndexMap = getAlgorithmToIndexMap()
    val algorithmVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(200, Array(algorithmToIndexMap.apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(200, Array(), Array())
      }
    }
    val algorithmFunc = udf(algorithmVector)
    val vAlgorithm = vCity.withColumn("algorithm_id_vector", algorithmFunc(col("algorithm_id")))
    vAlgorithm.show(5)

    //channel
    val channelToIndexMap = getChannelToIndexMap()
    val channelVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(60, Array(channelToIndexMap.apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(51, Array(), Array())
      }
    }
    val channelFunc = udf(channelVector)
    val vChannel = vAlgorithm.withColumn("channel_id_vector", channelFunc(col("channel_id")))//.cache()
    vChannel.show(5)

    //age
    val ageTrans = vChannel.withColumn("age_trans", vChannel("age")/50)
    ageTrans.show(5)

    //title_length
    val titleLengthTrans = ageTrans.withColumn("title_length_trans",ageTrans("title_length")/20)
    titleLengthTrans.show(5)

    //quality_score
    val quality_score_trans = titleLengthTrans.withColumn("quality_score_trans",titleLengthTrans("quality_score")/17)
    quality_score_trans.show(5)

    //cover picture
    val coverPicVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(3, Array(getCoverPicMap.apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(3, Array(), Array())
      }
    }
    val vCoverPicFunc = udf(coverPicVector)
    val vCoverPic = quality_score_trans.withColumn("cover_pic_vector", vCoverPicFunc(col("cover_pic")))//.cache()
    vCoverPic.show(5)

    val assemblerOutput = new VectorAssembler()
      .setInputCols(Array("hour_vector",  "gender_vector","age_trans",
        "city_level_vector", "algorithm_id_vector", "channel_id_vector",
        "login_freq", "avtive_level", "click_prefer", "stay_time",
        "title_length_trans", "cover_pic_vector", "quality_score_trans",
        "account_score", "attract_point", "read_time", "read_complete",
        "title_emotion", "title_attract", "channel_click_rate",
        "channel_like_rate", "channel_comment_rate", "algorithm_rate","is_holiday"))
      .setOutputCol("features")
      .transform(vCoverPic).select("article_id","features","label","uin")//.cache()

    assemblerOutput.show(5)
    assemblerOutput

  }

  def getAlgorithmToIndexMap(): Map[Long,Int] = {
    val spark = SparkSession.builder().getOrCreate()
    val props = new Properties()
    props.setProperty("user", "umqq2")
    props.setProperty("password", "pmqq_db")
    val df = spark.read.jdbc("jdbc:mysql://10.198.30.62:3392/MQQReading", "AlgorithmInfoNeiShou", props)
    val algorithmIds = df.select("AlgorithmID")
    val algorithmIds2 = algorithmIds.orderBy(asc("AlgorithmID"))

    algorithmIds2.show(1000)

    var map: Map[Long, Int] = Map()

    val arr = algorithmIds2.collect()

    var index = 0
    for (algorithmId <- arr) {
      map = map + (algorithmId.getLong(0) -> index)
      index = index + 1
    }
    map
  }

//    val map: Map[Int,Int] = Map(
//      1 -> 0,
//      2 -> 1,
//      3 -> 2,
//      4 -> 3,
//      5 -> 4,
//      6 -> 5,
//      7 -> 6,
//      8 -> 7,
//      9 -> 8,
//      10 -> 9,
//      11 -> 10,
//      12 -> 11,
//      13 -> 12,
//      14 -> 13,
//      15 -> 14,
//      16 -> 15,
//      17 -> 16,
//      18 -> 17,
//      19 -> 18,
//      20 -> 19,
//      21 -> 20,
//      22 -> 21,
//      23 -> 22,
//      24 -> 23,
//      25 -> 24,
//      26 -> 25,
//      27 -> 26,
//      28 -> 27,
//      29 -> 28,
//      30 -> 29,
//      31 -> 30,
//      32 -> 31,
//      33 -> 32,
//      34 -> 33,
//      35 -> 34,
//      36 -> 35,
//      37 -> 36,
//      38 -> 37,
//      39 -> 38,
//      40 -> 39,
//      41 -> 40,
//      42 -> 41,
//      43 -> 42,
//      44 -> 43,
//      45 -> 44,
//      46 -> 45,
//      48 -> 46,
//      49 -> 47,
//      124 -> 48,
//      145 -> 49,
//      146 -> 50,
//      147 -> 51,
//      148 -> 52,
//      149 -> 53,
//      700 -> 54,
//      1000 -> 55,
//      1001 -> 56,
//      1002 -> 57,
//      1003 -> 58,
//      1004 -> 59,
//      1005 -> 60,
//      1006 -> 61,
//      1007 -> 62,
//      1008 -> 63,
//      1009 -> 64,
//      1010 -> 65,
//      1011 -> 66,
//      1012 -> 67,
//      1013 -> 68,
//      1014 -> 69,
//      1015 -> 70,
//      1016 -> 71,
//      1017 -> 72,
//      1018 -> 73,
//      1019 -> 74,
//      1020 -> 75,
//      1021 -> 76,
//      1022 -> 77,
//      1023 -> 78,
//      1024 -> 79,
//      1025 -> 80,
//      1026 -> 81,
//      1027 -> 82,
//      1028 -> 83,
//      1030 -> 84,
//      1031 -> 85,
//      1032 -> 86,
//      1033 -> 87,
//      1034 -> 88,
//      1035 -> 89,
//      1036 -> 90,
//      1037 -> 91,
//      1038 -> 92,
//      1039 -> 93,
//      1040 -> 94,
//      1041 -> 95,
//      1042 -> 96,
//      1043 -> 97,
//      1044 -> 98,
//      1045 -> 99,
//      1046 -> 100,
//      1047 -> 101,
//      1048 -> 102,
//      1049 -> 103,
//      1050 -> 104,
//      1051 -> 105,
//      1052 -> 106,
//      1053 -> 107,
//      1054 -> 108,
//      1055 -> 109,
//      1056 -> 110,
//      1057 -> 111,
//      1058 -> 112,
//      1059 -> 113,
//      1060 -> 114,
//      1061 -> 115,
//      1062 -> 116,
//      1063 -> 117,
//      1064 -> 118,
//      1065 -> 119,
//      1066 -> 120,
//      1067 -> 121,
//      1068 -> 122,
//      1069 -> 123,
//      1070 -> 124,
//      1071 -> 125,
//      1072 -> 126,
//      1073 -> 127,
//      1074 -> 128,
//      1075 -> 129,
//      1076 -> 130,
//      1077 -> 131,
//      1078 -> 132,
//      1079 -> 133,
//      1080 -> 134,
//      1081 -> 135,
//      1082 -> 136,
//      1083 -> 137,
//      1084 -> 138,
//      1085 -> 139,
//      1086 -> 140,
//      1087 -> 141,
//      1088 -> 142,
//      1089 -> 143,
//      1090 -> 144,
//      1091 -> 145,
//      1092 -> 146,
//      1095 -> 147,
//      1096 -> 148,
//      1097 -> 149,
//      1098 -> 150,
//      1099 -> 151,
//      1100 -> 152,
//      1101 -> 153,
//      1400 -> 154,
//      1401 -> 155
//    )
//    map

  def getChannelToIndexMap(): Map[Int,Int] = {

    val map: Map[Int,Int] = Map(
      56 -> 0,
      73 -> 1,
      421 -> 2,
      500 -> 3,
      501 -> 4,
      547 -> 5,
      635 -> 6,
      658 -> 7,
      867 -> 8,
      1100 -> 9,
      1166 -> 10,
      1314 -> 11,
      2573 -> 12,
      2685 -> 13,
      2979 -> 14,
      3076 -> 15,
      3689 -> 16,
      3817 -> 17,
      5293 -> 18,
      5401 -> 19,
      5729 -> 20,
      5742 -> 21,
      7900 -> 22,
      11261 -> 23,
      11626 -> 24,
      12236 -> 25,
      12709 -> 26,
      14162 -> 27,
      19085 -> 28,
      53352 -> 29,
      506879 -> 30,
      552432 -> 31,
      552433 -> 32,
      552434 -> 33,
      552436 -> 34,
      552437 -> 35,
      552438 -> 36,
      552440 -> 37,
      552441 -> 38,
      552450 -> 39,
      552452 -> 40,
      552667 -> 41,
      552912 -> 42,
      553416 -> 43,
      553665 -> 44,
      554355 -> 45,
      554611 -> 46,
      557276 -> 47,
      559608 -> 48,
      562462 -> 49,
      649551 -> 50
    )
    map
  }

  def getCoverPicMap(): Map[Int,Int] = {

    val map: Map[Int,Int] = Map(
      0 -> 0,
      1 -> 1,
      3 -> 2
    )
    map
  }

}
