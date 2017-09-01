package com.tencent.kandian.ctr.dev

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, udf, _}

/**
  * Created by franniewu on 2017/7/13.
  */
object FeatureAnalysisHDFS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val data_small = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://tl-if-nn-tdw:54310/stage/outface/sng/sng_mediaaccount_app/franniewu/train_data/svm_train/bin24/20170620/")
    val data_large = MLUtils.loadLibSVMFile(spark.sparkContext, "hdfs://ss-sng-dc-v2/data/SPARK/SNG/g_sng_im_g_sng_mediaaccount/franniewu/train_data/svm_train/20170705/")
    
    for (fidx <- 0 to 430) {
      val data_small_col = data_small.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2.apply(fidx)))//.map(x => x._2)
      val data_large_col = data_large.map({case LabeledPoint(x,y) => (x,y)}).map(x => (x._1,x._2.apply(fidx)))//.map(x => x._2)

      val df_small = spark.createDataFrame(data_small_col)
      val df_large = spark.createDataFrame(data_large_col)

      val df_small_part0  = df_small.filter(df_small("_2").gt(0  ) && df_small("_2").lt(0.1) || df_small("_2").equalTo(0  ))
      val df_small_part1  = df_small.filter(df_small("_2").gt(0.1) && df_small("_2").lt(0.2) || df_small("_2").equalTo(0.1))
      val df_small_part2  = df_small.filter(df_small("_2").gt(0.2) && df_small("_2").lt(0.3) || df_small("_2").equalTo(0.2))
      val df_small_part3  = df_small.filter(df_small("_2").gt(0.3) && df_small("_2").lt(0.4) || df_small("_2").equalTo(0.3))
      val df_small_part4  = df_small.filter(df_small("_2").gt(0.4) && df_small("_2").lt(0.5) || df_small("_2").equalTo(0.4))
      val df_small_part5  = df_small.filter(df_small("_2").gt(0.5) && df_small("_2").lt(0.6) || df_small("_2").equalTo(0.5))
      val df_small_part6  = df_small.filter(df_small("_2").gt(0.6) && df_small("_2").lt(0.7) || df_small("_2").equalTo(0.6))
      val df_small_part7  = df_small.filter(df_small("_2").gt(0.7) && df_small("_2").lt(0.8) || df_small("_2").equalTo(0.7))
      val df_small_part8  = df_small.filter(df_small("_2").gt(0.8) && df_small("_2").lt(0.9) || df_small("_2").equalTo(0.8))
      val df_small_part9  = df_small.filter(df_small("_2").gt(0.9) && df_small("_2").lt(1  ) || df_small("_2").equalTo(0.9))
      val df_small_part10 = df_small.filter(df_small("_2").gt(1  ) && df_small("_2").lt(2  ) || df_small("_2").equalTo(1  ))
      val df_small_part11 = df_small.filter(df_small("_2").gt(2  ) && df_small("_2").lt(3  ) || df_small("_2").equalTo(2  ))
      val df_small_part12 = df_small.filter(df_small("_2").gt(3  ) && df_small("_2").lt(4  ) || df_small("_2").equalTo(3  ))
      val df_small_part13 = df_small.filter(df_small("_2").gt(4  ) && df_small("_2").lt(5  ) || df_small("_2").equalTo(4  ))
      val df_small_part14 = df_small.filter(df_small("_2").gt(5  ) && df_small("_2").lt(6  ) || df_small("_2").equalTo(5  ))
      val df_small_part15 = df_small.filter(df_small("_2").gt(6  ) && df_small("_2").lt(7  ) || df_small("_2").equalTo(6  ))
      val df_small_part16 = df_small.filter(df_small("_2").gt(7  ) && df_small("_2").lt(8  ) || df_small("_2").equalTo(7  ))
      val df_small_part17 = df_small.filter(df_small("_2").gt(8  ) && df_small("_2").lt(9  ) || df_small("_2").equalTo(8  ))
      val df_small_part18 = df_small.filter(df_small("_2").gt(9  )                           || df_small("_2").equalTo(9  ))

      val df_small_part0_pos  = df_small_part0.filter( df_small_part0("_1")  === 1)
      val df_small_part1_pos  = df_small_part1.filter( df_small_part1("_1")  === 1)
      val df_small_part2_pos  = df_small_part2.filter( df_small_part2("_1")  === 1)
      val df_small_part3_pos  = df_small_part3.filter( df_small_part3("_1")  === 1)
      val df_small_part4_pos  = df_small_part4.filter( df_small_part4("_1")  === 1)
      val df_small_part5_pos  = df_small_part5.filter( df_small_part5("_1")  === 1)
      val df_small_part6_pos  = df_small_part6.filter( df_small_part6("_1")  === 1)
      val df_small_part7_pos  = df_small_part7.filter( df_small_part7("_1")  === 1)
      val df_small_part8_pos  = df_small_part8.filter( df_small_part8("_1")  === 1)
      val df_small_part9_pos  = df_small_part9.filter( df_small_part9("_1")  === 1)
      val df_small_part10_pos = df_small_part10.filter(df_small_part10("_1") === 1)
      val df_small_part11_pos = df_small_part11.filter(df_small_part11("_1") === 1)
      val df_small_part12_pos = df_small_part12.filter(df_small_part12("_1") === 1)
      val df_small_part13_pos = df_small_part13.filter(df_small_part13("_1") === 1)
      val df_small_part14_pos = df_small_part14.filter(df_small_part14("_1") === 1)
      val df_small_part15_pos = df_small_part15.filter(df_small_part15("_1") === 1)
      val df_small_part16_pos = df_small_part16.filter(df_small_part16("_1") === 1)
      val df_small_part17_pos = df_small_part17.filter(df_small_part17("_1") === 1)
      val df_small_part18_pos = df_small_part18.filter(df_small_part18("_1") === 1)
      val df_small_part0_neg  = df_small_part0.filter( df_small_part0("_1")  === 0)
      val df_small_part1_neg  = df_small_part1.filter( df_small_part1("_1")  === 0)
      val df_small_part2_neg  = df_small_part2.filter( df_small_part2("_1")  === 0)
      val df_small_part3_neg  = df_small_part3.filter( df_small_part3("_1")  === 0)
      val df_small_part4_neg  = df_small_part4.filter( df_small_part4("_1")  === 0)
      val df_small_part5_neg  = df_small_part5.filter( df_small_part5("_1")  === 0)
      val df_small_part6_neg  = df_small_part6.filter( df_small_part6("_1")  === 0)
      val df_small_part7_neg  = df_small_part7.filter( df_small_part7("_1")  === 0)
      val df_small_part8_neg  = df_small_part8.filter( df_small_part8("_1")  === 0)
      val df_small_part9_neg  = df_small_part9.filter( df_small_part9("_1")  === 0)          
      val df_small_part10_neg = df_small_part10.filter(df_small_part10("_1") === 0)
      val df_small_part11_neg = df_small_part11.filter(df_small_part11("_1") === 0)
      val df_small_part12_neg = df_small_part12.filter(df_small_part12("_1") === 0)
      val df_small_part13_neg = df_small_part13.filter(df_small_part13("_1") === 0)
      val df_small_part14_neg = df_small_part14.filter(df_small_part14("_1") === 0)
      val df_small_part15_neg = df_small_part15.filter(df_small_part15("_1") === 0)
      val df_small_part16_neg = df_small_part16.filter(df_small_part16("_1") === 0)
      val df_small_part17_neg = df_small_part17.filter(df_small_part17("_1") === 0)
      val df_small_part18_neg = df_small_part18.filter(df_small_part18("_1") === 0)

      val df_large_part0  = df_large.filter(df_large("_2").gt(0  ) && df_large("_2").lt(0.1) || df_large("_2").equalTo(0  ))
      val df_large_part1  = df_large.filter(df_large("_2").gt(0.1) && df_large("_2").lt(0.2) || df_large("_2").equalTo(0.1))
      val df_large_part2  = df_large.filter(df_large("_2").gt(0.2) && df_large("_2").lt(0.3) || df_large("_2").equalTo(0.2))
      val df_large_part3  = df_large.filter(df_large("_2").gt(0.3) && df_large("_2").lt(0.4) || df_large("_2").equalTo(0.3))
      val df_large_part4  = df_large.filter(df_large("_2").gt(0.4) && df_large("_2").lt(0.5) || df_large("_2").equalTo(0.4))
      val df_large_part5  = df_large.filter(df_large("_2").gt(0.5) && df_large("_2").lt(0.6) || df_large("_2").equalTo(0.5))
      val df_large_part6  = df_large.filter(df_large("_2").gt(0.6) && df_large("_2").lt(0.7) || df_large("_2").equalTo(0.6))
      val df_large_part7  = df_large.filter(df_large("_2").gt(0.7) && df_large("_2").lt(0.8) || df_large("_2").equalTo(0.7))
      val df_large_part8  = df_large.filter(df_large("_2").gt(0.8) && df_large("_2").lt(0.9) || df_large("_2").equalTo(0.8))
      val df_large_part9  = df_large.filter(df_large("_2").gt(0.9) && df_large("_2").lt(1  ) || df_large("_2").equalTo(0.9))
      val df_large_part10 = df_large.filter(df_large("_2").gt(1  ) && df_large("_2").lt(2  ) || df_large("_2").equalTo(1  ))
      val df_large_part11 = df_large.filter(df_large("_2").gt(2  ) && df_large("_2").lt(3  ) || df_large("_2").equalTo(2  ))
      val df_large_part12 = df_large.filter(df_large("_2").gt(3  ) && df_large("_2").lt(4  ) || df_large("_2").equalTo(3  ))
      val df_large_part13 = df_large.filter(df_large("_2").gt(4  ) && df_large("_2").lt(5  ) || df_large("_2").equalTo(4  ))
      val df_large_part14 = df_large.filter(df_large("_2").gt(5  ) && df_large("_2").lt(6  ) || df_large("_2").equalTo(5  ))
      val df_large_part15 = df_large.filter(df_large("_2").gt(6  ) && df_large("_2").lt(7  ) || df_large("_2").equalTo(6  ))
      val df_large_part16 = df_large.filter(df_large("_2").gt(7  ) && df_large("_2").lt(8  ) || df_large("_2").equalTo(7  ))
      val df_large_part17 = df_large.filter(df_large("_2").gt(8  ) && df_large("_2").lt(9  ) || df_large("_2").equalTo(8  ))
      val df_large_part18 = df_large.filter(df_large("_2").gt(9  )                           || df_large("_2").equalTo(9  ))

      val df_large_part0_pos  = df_large_part0.filter( df_large_part0("_1")  === 1)
      val df_large_part1_pos  = df_large_part1.filter( df_large_part1("_1")  === 1)
      val df_large_part2_pos  = df_large_part2.filter( df_large_part2("_1")  === 1)
      val df_large_part3_pos  = df_large_part3.filter( df_large_part3("_1")  === 1)
      val df_large_part4_pos  = df_large_part4.filter( df_large_part4("_1")  === 1)
      val df_large_part5_pos  = df_large_part5.filter( df_large_part5("_1")  === 1)
      val df_large_part6_pos  = df_large_part6.filter( df_large_part6("_1")  === 1)
      val df_large_part7_pos  = df_large_part7.filter( df_large_part7("_1")  === 1)
      val df_large_part8_pos  = df_large_part8.filter( df_large_part8("_1")  === 1)
      val df_large_part9_pos  = df_large_part9.filter( df_large_part9("_1")  === 1)
      val df_large_part10_pos = df_large_part10.filter(df_large_part10("_1") === 1)
      val df_large_part11_pos = df_large_part11.filter(df_large_part11("_1") === 1)
      val df_large_part12_pos = df_large_part12.filter(df_large_part12("_1") === 1)
      val df_large_part13_pos = df_large_part13.filter(df_large_part13("_1") === 1)
      val df_large_part14_pos = df_large_part14.filter(df_large_part14("_1") === 1)
      val df_large_part15_pos = df_large_part15.filter(df_large_part15("_1") === 1)
      val df_large_part16_pos = df_large_part16.filter(df_large_part16("_1") === 1)
      val df_large_part17_pos = df_large_part17.filter(df_large_part17("_1") === 1)
      val df_large_part18_pos = df_large_part18.filter(df_large_part18("_1") === 1)
      val df_large_part0_neg  = df_large_part0.filter( df_large_part0("_1")  === 0)
      val df_large_part1_neg  = df_large_part1.filter( df_large_part1("_1")  === 0)
      val df_large_part2_neg  = df_large_part2.filter( df_large_part2("_1")  === 0)
      val df_large_part3_neg  = df_large_part3.filter( df_large_part3("_1")  === 0)
      val df_large_part4_neg  = df_large_part4.filter( df_large_part4("_1")  === 0)
      val df_large_part5_neg  = df_large_part5.filter( df_large_part5("_1")  === 0)
      val df_large_part6_neg  = df_large_part6.filter( df_large_part6("_1")  === 0)
      val df_large_part7_neg  = df_large_part7.filter( df_large_part7("_1")  === 0)
      val df_large_part8_neg  = df_large_part8.filter( df_large_part8("_1")  === 0)
      val df_large_part9_neg  = df_large_part9.filter( df_large_part9("_1")  === 0)
      val df_large_part10_neg = df_large_part10.filter(df_large_part10("_1") === 0)
      val df_large_part11_neg = df_large_part11.filter(df_large_part11("_1") === 0)
      val df_large_part12_neg = df_large_part12.filter(df_large_part12("_1") === 0)
      val df_large_part13_neg = df_large_part13.filter(df_large_part13("_1") === 0)
      val df_large_part14_neg = df_large_part14.filter(df_large_part14("_1") === 0)
      val df_large_part15_neg = df_large_part15.filter(df_large_part15("_1") === 0)
      val df_large_part16_neg = df_large_part16.filter(df_large_part16("_1") === 0)
      val df_large_part17_neg = df_large_part17.filter(df_large_part17("_1") === 0)
      val df_large_part18_neg = df_large_part18.filter(df_large_part18("_1") === 0)

      println("small f"+fidx+" in range\t [0   0.1)\t"+df_small_part0_pos.count() + "\t"+df_small_part0_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0   0.1)\t"+df_large_part0_pos.count() +"\t"+df_large_part0_neg.count())
      println("small f"+fidx+" in range\t [0.1 0.2)\t"+df_small_part1_pos.count() + "\t"+df_small_part1_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.1 0.2)\t"+df_large_part1_pos.count() +"\t"+df_large_part1_neg.count())
      println("small f"+fidx+" in range\t [0.2 0.3)\t"+df_small_part2_pos.count() + "\t"+df_small_part2_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.2 0.3)\t"+df_large_part2_pos.count() +"\t"+df_large_part2_neg.count())
      println("small f"+fidx+" in range\t [0.3 0.4)\t"+df_small_part3_pos.count() + "\t"+df_small_part3_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.3 0.4)\t"+df_large_part3_pos.count() +"\t"+df_large_part3_neg.count())
      println("small f"+fidx+" in range\t [0.4 0.5)\t"+df_small_part4_pos.count() + "\t"+df_small_part4_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.4 0.5)\t"+df_large_part4_pos.count() +"\t"+df_large_part4_neg.count())
      println("small f"+fidx+" in range\t [0.5 0.6)\t"+df_small_part5_pos.count() + "\t"+df_small_part5_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.5 0.6)\t"+df_large_part5_pos.count() +"\t"+df_large_part5_neg.count())
      println("small f"+fidx+" in range\t [0.6 0.7)\t"+df_small_part6_pos.count() + "\t"+df_small_part6_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.6 0.7)\t"+df_large_part6_pos.count() +"\t"+df_large_part6_neg.count())
      println("small f"+fidx+" in range\t [0.7 0.8)\t"+df_small_part7_pos.count() + "\t"+df_small_part7_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.7 0.8)\t"+df_large_part7_pos.count() +"\t"+df_large_part7_neg.count())
      println("small f"+fidx+" in range\t [0.8 0.9)\t"+df_small_part8_pos.count() + "\t"+df_small_part8_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.8 0.9)\t"+df_large_part8_pos.count() +"\t"+df_large_part8_neg.count())
      println("small f"+fidx+" in range\t [0.9   1)\t"+df_small_part9_pos.count() + "\t"+df_small_part9_neg.count() +"\t\t"+"large f"+fidx+" in range\t [0.9   1)\t"+df_large_part9_pos.count() +"\t"+df_large_part9_neg.count())
      println("small f"+fidx+" in range\t [1     2)\t"+df_small_part10_pos.count()+ "\t"+df_small_part10_neg.count()+"\t\t"+"large f"+fidx+" in range\t [1     2)\t"+df_large_part10_pos.count()+"\t"+df_large_part10_neg.count())
      println("small f"+fidx+" in range\t [2     3)\t"+df_small_part11_pos.count()+ "\t"+df_small_part11_neg.count()+"\t\t"+"large f"+fidx+" in range\t [2     3)\t"+df_large_part11_pos.count()+"\t"+df_large_part11_neg.count())
      println("small f"+fidx+" in range\t [3     4)\t"+df_small_part12_pos.count()+ "\t"+df_small_part12_neg.count()+"\t\t"+"large f"+fidx+" in range\t [3     4)\t"+df_large_part12_pos.count()+"\t"+df_large_part12_neg.count())
      println("small f"+fidx+" in range\t [4     5)\t"+df_small_part13_pos.count()+ "\t"+df_small_part13_neg.count()+"\t\t"+"large f"+fidx+" in range\t [4     5)\t"+df_large_part13_pos.count()+"\t"+df_large_part13_neg.count())
      println("small f"+fidx+" in range\t [5     6)\t"+df_small_part14_pos.count()+ "\t"+df_small_part14_neg.count()+"\t\t"+"large f"+fidx+" in range\t [5     6)\t"+df_large_part14_pos.count()+"\t"+df_large_part14_neg.count())
      println("small f"+fidx+" in range\t [6     7)\t"+df_small_part15_pos.count()+ "\t"+df_small_part15_neg.count()+"\t\t"+"large f"+fidx+" in range\t [6     7)\t"+df_large_part15_pos.count()+"\t"+df_large_part15_neg.count())
      println("small f"+fidx+" in range\t [7     8)\t"+df_small_part16_pos.count()+ "\t"+df_small_part16_neg.count()+"\t\t"+"large f"+fidx+" in range\t [7     8)\t"+df_large_part16_pos.count()+"\t"+df_large_part16_neg.count())
      println("small f"+fidx+" in range\t [8     9)\t"+df_small_part17_pos.count()+ "\t"+df_small_part17_neg.count()+"\t\t"+"large f"+fidx+" in range\t [8     9)\t"+df_large_part17_pos.count()+"\t"+df_large_part17_neg.count())
      println("small f"+fidx+" in range\t [9      )\t"+df_small_part18_pos.count()+ "\t"+df_small_part18_neg.count()+"\t\t"+"large f"+fidx+" in range\t [9      )\t"+df_large_part18_pos.count()+"\t"+df_large_part18_neg.count())
      println("\n")
    }
  }

}
