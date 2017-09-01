from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pytoolkit import TDBankProvider
from pytoolkit import TubeReceiverConfig

if __name__ == "__main__":
    """
    Usage: tdbank_dstream_test.py <master> <group> <topic> <tid>
    """
    if len(sys.argv) < 5:
        print("Usage: tdw_rdd_test.py <master> <group> <topic> <tid>")
        exit(1)

    master = sys.argv[1].strip()
    group = sys.argv[2].strip()
    topic = sys.argv[3].strip()
    tids = sys.argv[4].strip().split(",")

    sc = SparkContext(appName="tdbank-dstream-test")
    ssc = StreamingContext(sc, 1)
    tdbank = TDBankProvider(ssc)
    receiver_conf = TubeReceiverConfig()\
        .setMaster(master)\
        .setGroup(group)\
        .setTopic(topic)\
        .setTids(tids)
    stream = tdbank.textStream(receiver_conf, 1)

    def process(time, rdd):
        print("========= %s =========" % str(time))
        strs = rdd.take(1)
        print(strs)
        print("=====================")

    stream.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
