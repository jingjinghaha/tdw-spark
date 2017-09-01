package com.tencent.tdw.spark.examples

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdbank.{HippoReceiverConfig, TubeReceiverConfig, TDBankProvider}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by sharkdtu
 * 2015/9/9
 */
object TDBankDStreamTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    val streamingContext = new StreamingContext(sparkConf, Seconds(30))

    val cmdArgs = new GeneralArgParser(args)

    val master = cmdArgs.getStringValue("master")
    val group = cmdArgs.getStringValue("group")
    val topic = cmdArgs.getStringValue("topic")
    val tids = cmdArgs.getCommaSplitArrayValue("tids")

    val tdbank = new TDBankProvider(streamingContext)
    // From Tube
    val receiverConf = new TubeReceiverConfig().setMaster(master)
      .setGroup(group).setTids(tids).setTopic(topic)
    // From hippo
    // val receiverConf = new HippoReceiverConfig().setControllerAddrs(addrs)
    //  .setGroup(group).setTopic(topic)
    val stream = tdbank.bytesStream(receiverConf, numReceiver = 1)

//    stream.foreachRDD{ rdd =>
//      val ret = rdd.take(2)
//      for(ba <- ret){
//        println(new String(ba))
//      }
//    }

    val cnt = stream.count().map{ x =>
      "count at " + System.currentTimeMillis + ": " + x
    }
    cnt.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
