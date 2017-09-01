package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

object calcAUC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)
//    val rocDataPath: String = cmdArgs.getStringValue("rocPath")
    //
    //    val data = spark.read.textFile(rocDataPath)
    //    val predictionAndLabels = data.rdd.map(_.split(" ")).map(x => (x.apply(1).toDouble, x.apply(0).toDouble))
    //    //println(predictionAndLabels.first())
    //
    //    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    //    val auc = metrics.areaUnderROC()
    //    println(auc)

    val predResultPath: String = cmdArgs.getStringValue("predResultPath")

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

    val rocData = spark.read.text(predResultPath).rdd.map(r => getPredictData(r.getString(0)))
      .filter(_._2 > -0.5)
      .map(r => (r._1, r._2))

    val metrics = new BinaryClassificationMetrics(rocData)
    val auc = metrics.areaUnderROC()
    println(auc)


  }
}
