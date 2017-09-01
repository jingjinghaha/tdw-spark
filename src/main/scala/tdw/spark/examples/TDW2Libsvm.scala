package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWProvider
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by sharkdtu
 * 2016/5/5.
 */
object TDW2Libsvm {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val cmdArgs = new GeneralArgParser(args)

    val srcTbl = cmdArgs.getStringValue("src_tbl")
    val priParts = cmdArgs.getCommaSplitArrayValue("pri_parts")
    val subParts = cmdArgs.getCommaSplitArrayValue("sub_parts")
    // Dest hdfs path
    val dstPath = cmdArgs.getStringValue("dst_path")

    val tdw = new TDWProvider(sc, "test_user", "test_passwd", "test_db")
    // Suppose, first field is label,
    val data = tdw.table(srcTbl, priParts, subParts).map{ x =>
      s"${x(0)} 1:${x(1)} 2:${x(2)}"
    }
    data.saveAsTextFile(dstPath)
  }
}
