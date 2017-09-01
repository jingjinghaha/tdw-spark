from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.serializers import UTF8Deserializer
from pyspark.serializers import NoOpSerializer

from provider import encodeUTF8


def loadRDDFromTable(sc, url, use_unicode=True):
    jrdd = sc._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.loadTable(sc._jsc, url)
    return RDD(jrdd, sc, UTF8Deserializer(use_unicode)).map(lambda x: x.split("\01", -1))


def saveRDDToTable(rdd, url, overwrite=True):
    encoded = rdd.map(lambda x: "\01".join(x)).mapPartitions(encodeUTF8)
    encoded._bypass_serializer = True
    rdd.context._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.\
        saveToTable(encoded._jrdd.map(rdd.context._jvm.BytesToString()), url, overwrite)


def loadDataFrameFromTable(session, url):
    df = session._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.loadTable(session._jsparkSession, url)
    return DataFrame(df, session._wrapped)


def saveDataFrameToTable(df, url, overwrite=True):
    df.sql_ctx._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.\
        saveToTable(df._jdf, url, overwrite)


def loadProtobufTable(sc, url):
    jrdd = sc._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.loadProtobufTable(sc._jsc, url)
    return RDD(jrdd, sc, NoOpSerializer())


def saveToProtobufTable(rdd, url, overwrite=True):
    rdd._bypass_serializer = True
    rdd.context._jvm.\
        com.tencent.tdw.spark.toolkit.api.python.PythonTDWFunctions.\
        saveToProtobufTable(rdd._jrdd, url, overwrite)

