package com.tencent.kandian.ctr.dev

import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.SparkSession

/**
  * Created by franniewu on 2017/5/5.
  */
object MyGBDT {

  def main(args: Array[String]) {

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")

    val data = tdw.table("simsonhe_kandian_train_data", DateUtil.getTodayDate(dateStr))

    val assemblerOutput = DataProcessing.transFeatureMapping(data,dateStr)

    val paramSegment:List[String] = List("0.00001")

    for(item<-paramSegment) {
      //val itemSplit = item.split(",");

      // Split the data into training and test sets (30% held out for testing).
      val Array(trainingDataTmp, testDataTmp) = assemblerOutput.randomSplit(Array(0.7, 0.3))

      val trainingData = trainingDataTmp.repartition(50)
      val testData = testDataTmp.repartition(50)

      // Train a GBT model.
      val gbt = new GBTRegressor()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(30) //num of tree?
        .setMaxDepth(3)
        .setSubsamplingRate(1)
        .setMinInfoGain(item.toDouble)
        .setStepSize(0.05) //learning rate

      // Chain indexer and GBT in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(gbt))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(trainingData)

      // Make predictions.
      val predictions = model.transform(testData).repartition(50)//.cache()


      // Select (prediction, true label) and compute test error.
      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)

      val timeStr = getCurrent_time()

      println("params:"+30+"  "+3+"  "+item)
      println("Model name: gbdt_model_" + timeStr)
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)

      //===========================================增加uin过滤
      val preColumn = predictions.select("prediction", "label", "uin")
      val positiveRow = preColumn.filter(preColumn("label").equalTo(1)).select("prediction", "uin").toDF("posi_pre", "posi_uin")
      val nativeRow = preColumn.filter(preColumn("label").equalTo(0)).select("prediction", "uin").toDF("nagi_pre", "nagi_uin")
      val crossJoin = positiveRow.join(nativeRow, positiveRow("posi_uin") === nativeRow("nagi_uin")).cache()

      val tatolRowCnt = crossJoin.count()
      val goodRowCnt = crossJoin.filter(crossJoin("posi_pre") > crossJoin("nagi_pre")).count()
      val aucRet = goodRowCnt.toDouble / tatolRowCnt

      println("auc:" + aucRet)

      val gbtModel = model.stages(0).asInstanceOf[GBTRegressionModel]


      //println("Learned regression GBT model:\n" + gbtModel.toDebugString)
      println("Feature importances:" + gbtModel.featureImportances)
      // Save and load model
      gbtModel.save("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/GBDTModel/gbdt_model_" + timeStr)

      // save the result and parameters to the train_result table
      // val retDf = spark.createDataFrame(List((dateStr, ("gbdt_model_" + timeStr), rmse, aucRet, ("tree num:"+30+",tree height:"+3+",minGain:"+item), gbtModel.featureImportances.toString)))
      //   .toDF("date_key","model_name","rmse","auc","tree","feature_importances").withColumn("no",lit(1))

      // tdw.saveToTable(retDf.select("no","date_key","model_name","rmse","auc","tree","feature_importances"), "simsonhe_kandian_train_result","p_1", null, false)

    }
  }

  def getCurrent_time(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss")
    val timeStr = dateFormat.format(now)
    timeStr
  }

}
