package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.parsing.json.JSON
/**
  * Created by Administrator on 2017/7/19.
  */
object GetRocData {
  def main(args:Array[String]) = {

    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)
    val predResultPath: String = cmdArgs.getStringValue("predResultPath")
    //val rocDataPath: String = cmdArgs.getStringValue("rocPath")

    def getPredictData(jsonStr: String): (Double, Double) = {
      try {
        val jsonData: Option[Any] = JSON.parseFull(jsonStr)
        val pair: Map[String, Any] = jsonData.get.asInstanceOf[Map[String, Any]]
        val label: Double = pair.getOrElse("label", null).asInstanceOf[Double]
        val probabilities: Map[String, Any] = pair.getOrElse("probabilities", null).asInstanceOf[Map[String, Any]]
        val values: List[Double] = probabilities.getOrElse("values", null).asInstanceOf[List[Double]]
        val predict = values(1)
        (predict, label)
      } catch {
        case ex: Throwable =>  (-1.0, 0.0)
      }
    }

//    def getPredictData(jsonStr: String): (Double, Double) = {
//      try {
//        val jsonData: Option[Any] = JSON.parseFull(jsonStr)
//        val pair: Map[String, Any] = jsonData.get.asInstanceOf[Map[String, Any]]
//        val label: Double = pair.getOrElse("label", null).asInstanceOf[Double]
//        val predict: Double = pair.getOrElse("prediction", null).asInstanceOf[Double]
//        return (label, predict)
//      } catch {
//        case ex: Throwable => return (-1.0, 0.0)
//      }
//    }

    val rocData = spark.read.text(predResultPath).rdd.map(r => getPredictData(r.getString(0)))
      .filter(_._2 > -0.5)
      .map(r => (r._1, r._2))

    val metrics = new BinaryClassificationMetrics(rocData)
    val auc = metrics.areaUnderROC()
    println(auc)

//    val rocDataSchema = StructType(
//      Array(
//        StructField("label", DoubleType), StructField("predict", DoubleType)
//      )
//    )
//    val rocDf = spark.createDataFrame(rocData, rocDataSchema)
//
//    rocDf.write
//      .format("csv")
//      .option("head", false)
//      .option("delimiter", " ")
//      .mode("overwrite")
//      .save(rocDataPath)
  }
}
