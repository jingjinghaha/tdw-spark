package com.tencent.kandian.ctr.dev

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by franniewu on 2017/7/22.
  */
object SampleRate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val data = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_train/20170719/")

    val df = spark.createDataFrame(data.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2)))

    val ratioList: List[Long] = List(2, 4, 6, 8, 10)

    for (i <- ratioList) {
      val ratio = 0.1*i

      val Array(partition1, partition2) = df.randomSplit(Array(ratio, 1-ratio))

      val sampledData = MLUtils.convertVectorColumnsToML(partition1)

      val realRatio = i/2
      sampledData.write.format("libsvm").mode("overwrite")
        .save("hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_train/all_" + realRatio + "/20170719")

    }



  }
}
