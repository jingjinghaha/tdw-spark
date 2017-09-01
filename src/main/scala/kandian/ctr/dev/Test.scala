package com.tencent.kandian.ctr.dev

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions.{col, udf}
/**
  * Created by franniewu on 2017/7/19.
  */
object Test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().getOrCreate()

    val data = MLUtils.loadLibSVMFile(spark.sparkContext, "data/")
    val df = spark.createDataFrame(data)
    df.show()

    val df_col = spark.createDataFrame(data.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2.apply(0))))
    df_col.show()

    val data1 = MLUtils.loadLibSVMFile(spark.sparkContext, "train_svm_20170718_part_09999") // 1156 columns
    val df1 = spark.createDataFrame(data1)
    df1.show()

    val data2 = MLUtils.loadLibSVMFile(spark.sparkContext, "train_svm_20170705_part_03999") // 1160 columns
    val df2 = spark.createDataFrame(data2)
    df2.show()

    val data3 = MLUtils.loadLibSVMFile(spark.sparkContext, "train_svm_20170711_part_03999") // 1160 columns
    val df3 = spark.createDataFrame(data3)
    df3.show()

    for (i <- 1 to 10){
      println(i)
    }

    val data4 = spark.read.json("pred_result/xaa")
    data4.show(10, false)

//    val prob: ([BigInt, Array[Double]] => Double) = (arg: [BigInt, Array[Double]]) => {
//      println(arg.apply(0))
//      val prob_1 = arg.apply(1)
//      prob_1
//    }
//    val probFunc = udf(prob)
//    val df4 = data4.withColumn("prob", probFunc(col("probabilities")))
//    df4.show(10, false)

    val parameters = DateUtil.getOneWeekString("20170727")
    for (date <- parameters){ println(date)}


    val data5 = spark.read.textFile("pred_result/")
    val predictionAndLabels = data5.rdd.map(_.split(" ")).map(x => (x.apply(1).toDouble, x.apply(0).toDouble))
    println(predictionAndLabels.first())
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    val auc = metrics.areaUnderROC()
    println(auc)

  }
}
