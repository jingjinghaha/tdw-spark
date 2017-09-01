package com.tencent.kandian.ctr.dev

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by franniewu on 2017/7/20.
  */
object ParseJsonPredResult {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val pred_result = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://tl-if-nn-tdw.tencent-distribute.com:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/xgboost_trail/bin24/pred_result")

  }
}
