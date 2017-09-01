package com.tencent.kandian.ctr.dev

import org.apache.spark.sql.functions
import com.tencent.tdw.spark.examples.util.GeneralArgParser
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by franniewu on 2017/7/18.
  */
object PosNegRatio {

  def main(args: Array[String]): Unit = {
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)
    println(DateUtil.getOneWeek(dateStr))

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")
    val ratio: Double = cmdArgs.getDoubleValue("ratio")
    println(ratio)

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/svm_train/bin24/20170705/")
    val df = spark.createDataFrame(data.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2)))

    val df_pos = df.filter(df("_1").equalTo(1.0))
    val df_neg = df.filter(df("_1").equalTo(0.0))
    println(df_pos.count(), df_neg.count())

    val r = df_neg.count().toDouble/df_pos.count().toDouble
    println(r)

    val Array(partition1, partition2) = df_neg.randomSplit(Array(ratio/r, 1-ratio/r))
    println(partition1.count(), partition2.count())

    val train_data = df_pos.union(partition1)
    train_data.show()
    println(train_data.count())

    val final_data = MLUtils.convertVectorColumnsToML(train_data)

    final_data.write.format("libsvm").mode("overwrite")
      .save("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/svm_train/bin24/20170705_"+ratio)

  }
}
