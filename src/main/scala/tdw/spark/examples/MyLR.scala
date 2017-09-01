package com.tencent.tdw.spark.examples

import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MyLR {

  def main(args: Array[String]) {

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    // val data = tdw.table("simsonhe_kandian_train_data_vector", Seq("p_" + dateStr)).select("uin","feature_vector","label")

    val data = tdw.table("simsonhe_kandian_train_data", Seq("p_" + dateStr))//.select("date_key","hour","gender","age","city_level","login_freq","avtive_level","click_prefer","stay_time","title_length","cover_pic","quality_score","account_score","attract_point","read_time","read_complete","title_emotion","title_attract","channel_click_rate","channel_like_rate","channel_comment_rate","channel_id","algorithm_rate","algorithm_id","label","uin","article_id")

//    val data2 = data.filter(data("avtive_level").gt(0.8))
//
//    val newData = data2.sqlContext.createDataFrame(data2.rdd, StructType(data2.schema.map(_.copy(nullable = false))))
//
//    //val newData2 = newData.repartition(10);
//
//    val newData2_posi = newData.filter(newData("label") === 1)
//    val newData2_nagi = newData.filter(newData("label") === 0)
//
//    val Array(newData2_nagi_left, newData2_nagi_ignore) = newData2_nagi.randomSplit(Array(0.3, 0.7))
//
//    val newData3 = newData2_posi.union(newData2_nagi_left) //.repartition(10)

    val newData3 = DataProcessing.transTrainData(data)

    val assemblerOutput = DataProcessing.transFeature(newData3)


    ///////////////=============================

    val Array(trainingDataTmp, testDataTmp) = assemblerOutput.randomSplit(Array(0.7, 0.3))

    val trainingData = trainingDataTmp.repartition(20)
    val testData = testDataTmp.repartition(20)


    val paramSegment:List[String] = List("0.01,1,10")
    //
    for(item<-paramSegment) {

      val itemSplit = item.split(",");

      val lr = new LinearRegression()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(itemSplit.apply(2).toInt)
        .setRegParam(itemSplit.apply(0).toDouble)
        .setElasticNetParam(itemSplit.apply(1).toDouble)

      // Chain indexer and LR in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(lr))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(trainingData)

      // Make predictions.
      val predictions = model.transform(testData).repartition(10) //.cache()

      // Select example rows to display.
      //predictions.select("prediction", "label", "features").show(5)
      //predictions.show(5)
      val lrModel = model.stages(0).asInstanceOf[LinearRegressionModel]



      // Select (prediction, true label) and compute test error.
      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)

      val timeStr = getCurrent_time()

      //println("params:"+itemSplit.apply(0).toInt+"  "+itemSplit.apply(1).toInt)
      println("Model name: lr_model_" + timeStr)
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)

      //===========================================增加uin过滤
      val preColumn = predictions.select("prediction", "label", "uin")
      val positiveRow = preColumn.filter(preColumn("label").equalTo(1)).select("prediction", "uin").toDF("posi_pre", "posi_uin")
      val nativeRow = preColumn.filter(preColumn("label").equalTo(0)).select("prediction", "uin").toDF("nagi_pre", "nagi_uin")
      val crossJoin = positiveRow.join(nativeRow, positiveRow("posi_uin") === nativeRow("nagi_uin")).cache()

      val tatolRowCnt = crossJoin.count()
      println("tatolRowCnt:" + tatolRowCnt)
      val goodRowCnt = crossJoin.filter(crossJoin("posi_pre") > crossJoin("nagi_pre")).count()
      println("goodRowCnt:" + goodRowCnt)
      val aucRet = goodRowCnt.toDouble / tatolRowCnt

      println("auc:" + aucRet)

      lrModel.save("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/simsonhe_ml_model/LRModel/lr_model_" + timeStr)

      val modelRemark = "iter:"+itemSplit.apply(2)+",RegParam:"+itemSplit.apply(0)+",ElasticNetParam:"+itemSplit.apply(1)
      val retDf = spark.createDataFrame(List((dateStr, ("lr_model_" + timeStr), rmse, aucRet, modelRemark, "")))
        .toDF("date_key", "model_name", "rmse", "auc", "tree", "feature_importances").withColumn("no", lit(2))

      tdw.saveToTable(retDf.select("no", "date_key", "model_name", "rmse", "auc", "tree", "feature_importances"), "simsonhe_kandian_train_result", "p_2", null, false)
    }
    ///===========================================================================

  }

  def getCurrent_time(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss")
    var timeStr = dateFormat.format(now)
    timeStr
  }

  def getAlgorithmToIndexMap(): Map[Int, Int] = {

    val map: Map[Int, Int] = Map(
      1 -> 0,
      2 -> 1,
      3 -> 2,
      4 -> 3,
      5 -> 4,
      6 -> 5,
      7 -> 6,
      8 -> 7,
      9 -> 8,
      10 -> 9,
      11 -> 10,
      12 -> 11,
      13 -> 12,
      14 -> 13,
      15 -> 14,
      16 -> 15,
      17 -> 16,
      18 -> 17,
      19 -> 18,
      20 -> 19,
      21 -> 20,
      22 -> 21,
      23 -> 22,
      24 -> 23,
      25 -> 24,
      26 -> 25,
      27 -> 26,
      28 -> 27,
      29 -> 28,
      30 -> 29,
      31 -> 30,
      32 -> 31,
      33 -> 32,
      34 -> 33,
      35 -> 34,
      36 -> 35,
      37 -> 36,
      38 -> 37,
      39 -> 38,
      40 -> 39,
      41 -> 40,
      42 -> 41,
      43 -> 42,
      44 -> 43,
      45 -> 44,
      46 -> 45,
      48 -> 46,
      49 -> 47,
      124 -> 48,
      145 -> 49,
      146 -> 50,
      147 -> 51,
      148 -> 52,
      149 -> 53,
      700 -> 54,
      1000 -> 55,
      1001 -> 56,
      1002 -> 57,
      1003 -> 58,
      1004 -> 59,
      1005 -> 60,
      1006 -> 61,
      1007 -> 62,
      1008 -> 63,
      1009 -> 64,
      1010 -> 65,
      1011 -> 66,
      1012 -> 67,
      1013 -> 68,
      1014 -> 69,
      1015 -> 70,
      1016 -> 71,
      1017 -> 72,
      1018 -> 73,
      1019 -> 74,
      1020 -> 75,
      1021 -> 76,
      1022 -> 77,
      1023 -> 78,
      1024 -> 79,
      1025 -> 80,
      1026 -> 81,
      1027 -> 82,
      1028 -> 83,
      1030 -> 84,
      1031 -> 85,
      1032 -> 86,
      1033 -> 87,
      1034 -> 88,
      1035 -> 89,
      1036 -> 90,
      1037 -> 91,
      1038 -> 92,
      1039 -> 93,
      1040 -> 94,
      1041 -> 95,
      1042 -> 96,
      1043 -> 97,
      1044 -> 98,
      1045 -> 99,
      1046 -> 100,
      1047 -> 101,
      1048 -> 102,
      1049 -> 103,
      1050 -> 104,
      1051 -> 105,
      1052 -> 106,
      1053 -> 107,
      1054 -> 108,
      1055 -> 109,
      1056 -> 110,
      1057 -> 111,
      1058 -> 112,
      1059 -> 113,
      1060 -> 114,
      1061 -> 115,
      1062 -> 116,
      1063 -> 117,
      1064 -> 118,
      1065 -> 119,
      1066 -> 120,
      1067 -> 121,
      1068 -> 122,
      1069 -> 123,
      1070 -> 124,
      1071 -> 125,
      1072 -> 126,
      1073 -> 127,
      1074 -> 128,
      1075 -> 129,
      1076 -> 130,
      1077 -> 131,
      1078 -> 132,
      1079 -> 133,
      1080 -> 134,
      1081 -> 135,
      1082 -> 136,
      1083 -> 137,
      1084 -> 138,
      1085 -> 139,
      1086 -> 140,
      1087 -> 141,
      1088 -> 142,
      1089 -> 143,
      1090 -> 144,
      1091 -> 145,
      1092 -> 146,
      1095 -> 147,
      1096 -> 148,
      1097 -> 149,
      1098 -> 150,
      1099 -> 151,
      1100 -> 152,
      1101 -> 153,
      1400 -> 154,
      1401 -> 155
    )
    return map
  }

  def getChannelToIndexMap(): Map[Int, Int] = {

    val map: Map[Int, Int] = Map(
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
    return map
  }

}