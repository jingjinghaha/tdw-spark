package com.tencent.kandian.ctr.dev

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by franniewu on 2017/7/21.
  */
object PosNegCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

//    val data_small = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/svm_train/bin24/20170620/")
    val data_large = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_train/20170719/")

//    val df_small = spark.createDataFrame(data_small.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2)))
//    val df_small_pos = df_small.filter(df_small("_1").equalTo(1.0))
//    val df_small_neg = df_small.filter(df_small("_1").equalTo(0.0))
//    println("small the positive and negative count are: " + df_small_pos.count() + ", " + df_small_neg.count())
//    val r1 = df_small_neg.count().toDouble/df_small_pos.count().toDouble
//    println("small the negative / positive ratio is: " + r1)

    val df_large = spark.createDataFrame(data_large.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2)))
    val df_large_pos = df_large.filter(df_large("_1").equalTo(1.0))
    val df_large_neg = df_large.filter(df_large("_1").equalTo(0.0))
    println("large the positive and negative count are: " + df_large_pos.count() + ", " + df_large_neg.count())
    val r2 = df_large_neg.count().toDouble/df_large_pos.count().toDouble
    println("large the negative / positive ratio is: " + r2)
    
  }
}
