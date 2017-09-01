package com.tencent.kandian.ctr.dev

import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.tdw.spark.examples.MyLR.getCurrent_time
import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.types.StructType

/**
  * Created by franniewu on 2017/5/22.
  */
object SparkLr {

  def main(args:Array[String]): Unit ={
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val data = tdw.table("simsonhe_kandian_train_data", Seq("p_" + dateStr))

    val filtered_data = data.filter(
      data("gender").equalTo(2) &&
        data("age").gt(15) && data("age").lt(19) &&
        data("avtive_level").gt(0) &&
        data("date_key").isNotNull &&
        data("hour").isNotNull &&
        data("city_level").isNotNull &&
        data("stay_time").isNotNull &&
        data("title_length").isNotNull &&
        data("cover_pic").isNotNull &&
        data("quality_score").isNotNull &&
        data("account_score").isNotNull &&
        data("attract_point").isNotNull &&
        data("read_time").isNotNull &&
        data("read_complete").isNotNull &&
        data("title_emotion").isNotNull &&
        data("title_attract").isNotNull &&
        data("channel_click_rate").isNotNull &&
        data("channel_like_rate").isNotNull &&
        data("channel_comment_rate").isNotNull &&
        data("channel_id").isNotNull &&
        data("algorithm_rate").isNotNull &&
        data("algorithm_id").isNotNull &&
        data("uin").isNotNull &&
        data("article_id").isNotNull &&
        data("di_score").isNotNull &&
        data("di_usermodel_score").isNotNull &&
        data("di_tag_score").isNotNull &&
        data("di_hqtag_score").isNotNull &&
        data("account_read_score").isNotNull &&
        data("account_click_score").isNotNull &&
        data("account_like_score").isNotNull &&
        data("account_comment_score").isNotNull &&
        data("account_health_score").isNotNull &&
        data("account_res_score").isNotNull)

    val transData = DataProcessing.transFeatureMapping(filtered_data, dateStr)  //4个字段："article_id","features","label","uin"；其中features是vector

    val userProfileTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val userProfileData = userProfileTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + dateStr))
      .select("tdbank_imp_date", "uin", "potrait").toDF("tdbank_imp_date", "uin2", "potrait").cache()
    userProfileData.show(5)
    // 去重，it is unsure whether the first row or the last row is kept.
    val userProfileRows = userProfileData.filter(userProfileData("potrait").isNotNull)
      .rdd.map(r => (r.get(1), r)).reduceByKey((r0, r1) => r0).map(r => r._2).map(r => Row.fromSeq(r.toSeq))
    val userTagSchema = StructType(userProfileData.schema.fields)
    val userProfileDF = spark.createDataFrame(userProfileRows, userTagSchema)
    userProfileDF.show(5)

    val filteredProfile = filtered_data.join(userProfileDF, filtered_data("uin") === userProfileDF("uin2"),"left_outer").cache()
    println("the number of instance in train data: " + filtered_data.count())
    println("the number of instance after join user profile: " + filteredProfile.count())

    // 4个字段："article_id","features","label","uin"；其中features是vector
    val newData = DataProcessing.transFeatureMapping(filteredProfile, dateStr).select("label","features").cache()

    val Array(trainingDataTmp, testDataTmp) = newData.randomSplit(Array(0.7, 0.3))

    val trainingData = trainingDataTmp.repartition(20)
    val testData = testDataTmp.repartition(20)

    val paramSegment:List[String] = List("0.01,1,10")
    for(item<-paramSegment) {
      val itemSplit = item.split(",")
      val lr = new LogisticRegression()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(itemSplit.apply(2).toInt)
        .setRegParam(itemSplit.apply(0).toDouble)
        .setElasticNetParam(itemSplit.apply(1).toDouble)

      // Chain indexer and LR in a Pipeline.
      val pipeline = new Pipeline().setStages(Array(lr))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(trainingData)


      // Make predictions.
      val predictions = model.transform(testData).repartition(10) //.cache()
      //predictions.select("prediction", "label", "features").show(5)
      predictions.show(5)

      val lrModel = model.stages(0).asInstanceOf[LinearRegressionModel]

      // Print the coefficients and intercept for logistic regression
      println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

      // Select (prediction, true label) and compute test error.
      val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
      val rmse = evaluator.evaluate(predictions)

      val timeStr = getCurrent_time(0)

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

  }

  def getCurrent_time(int: Int): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH_mm_ss")
    val timeStr = dateFormat.format(now)
    timeStr
  }

}
