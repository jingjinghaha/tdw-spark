package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object OneWeekDataFromHDFS {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    val dateList = DateUtil.getOneWeekString(dateStr)

    for (date <- dateList){
      val piece = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_train/feature_v5/sampled/" + date)
      val df = spark.createDataFrame(piece.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2)))
      val data = MLUtils.convertVectorColumnsToML(df)
      data.write.format("libsvm").mode("append").save("hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_train/feature_v5/sampled_week/"+dateStr)
    }

    for (date <- dateList){
      val piece = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_test/feature_v5/sampled/" + date)
      val df = spark.createDataFrame(piece.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2)))
      val data = MLUtils.convertVectorColumnsToML(df)
      data.write.format("libsvm").mode("append").save("hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_test/feature_v5/sampled_week/"+dateStr)
    }

  }
}
