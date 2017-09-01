package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession


object ConvertToLibsvm {

  def main(args: Array[String]) {

    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    print(dateStr)

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val data = tdw.table("simsonhe_kandian_train_data", Seq("p_" + dateStr))

    val transData = DataProcessing.transTrainData(data)  //4个字段："article_id","features","label","uin"；其中features是vector

    val newData = transData.select("label","features")
    println(newData.count())
    newData.printSchema()
    newData.show(5)

    //show one element of features
    val featureCol = newData("features")
    print("one element of feature vector is: " + featureCol(0).toString())

    // convert DataFrame columns
    val convertedVecDF = MLUtils.convertVectorColumnsToML(newData)
    convertedVecDF.printSchema()
    convertedVecDF.show(5)

    // random split the dataset
    val Array(train_data, test_data) = convertedVecDF.randomSplit(Array(0.8, 0.2))
    train_data.show(5)
    test_data.show(5)

    val train_data_pos = train_data.filter(train_data("label")===1)
    println("The number of positive instance in train dataset: " + train_data_pos.count())
    val train_data_neg = train_data.filter(train_data("label")===0)
    println("The number of negative instance in train dataset: " + train_data_neg.count())
    val ratio_train = train_data_pos.count()/train_data_neg.count()
    println("The ratio of pos/neg in train dataset: " + ratio_train)

    val test_data_pos = test_data.filter(test_data("label")===1)
    println("The number of positive instance in test dataset: " + test_data_pos.count())
    val test_data_neg = test_data.filter(test_data("label")===0)
    println("The number of negative instance in test dataset: " + test_data_neg.count())
    val ratio_test = test_data_pos.count()/test_data_neg.count()
    println("The ratio of pos/neg in test dataset: " + ratio_test)

    //    0.0 1:1.0 3:3.0
    //    1.0 1:1.0 2:0.0 3:3.0
    // convertedVecDF.write.format("libsvm").mode("overwrite").save("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/svm")
    train_data.write.format("libsvm").mode("overwrite").save("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/svm_train/"+dateStr)
    test_data.write.format("libsvm").mode("overwrite").save("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/svm_test/"+dateStr)

    //tring to store dataframe to text file, but failed as the dataframe contains vertor
//    val train_out = new VectorAssembler()
//      .setInputCols(Array("label",  "features"))
//      .setOutputCol("instance")
//      .transform(train_data).select("instance")
//
//    val test_out = new VectorAssembler()
//      .setInputCols(Array("label",  "features"))
//      .setOutputCol("instance")
//      .transform(test_data).select("instance")
//    train_out.write.mode("overwrite").text("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/dense_train")
//    test_out.write.mode("overwrite").text("hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/dense_test")
  }

}