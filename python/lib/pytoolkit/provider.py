from pyspark.rdd import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.sql.dataframe import DataFrame
from pyspark.serializers import UTF8Deserializer
from pyspark.serializers import NoOpSerializer
from pyspark.streaming.context import DStream


class ReceiverConfig(object):
    def __init__(self):
        self._group = None
        self._topic = None
        self._consumeFromMaxOffset = True
        self._storageLevel = StorageLevel.MEMORY_AND_DISK_2

    @property
    def group(self):
        return self._group

    def setGroup(self, group):
        self._group = group
        return self

    @property
    def topic(self):
        return self._topic

    def setTopic(self, topic):
        self._topic = topic
        return self

    @property
    def consumeFromMaxOffset(self):
        return self._consumeFromMaxOffset

    def setConsumeFromMaxOffset(self, consumeFromMaxOffset):
        self._consumeFromMaxOffset = consumeFromMaxOffset
        return self

    @property
    def storageLevel(self):
        return self._storageLevel

    def setStorageLevel(self, storageLevel):
        self._storageLevel = storageLevel
        return self


class TubeReceiverConfig(ReceiverConfig):
    def __init__(self):
        super(TubeReceiverConfig, self).__init__()
        self._master = None
        self._tids = None
        self._includeTid = False

    @property
    def master(self):
        return self._master

    def setMaster(self, master):
        self._master = master
        return self

    @property
    def tids(self):
        return self._tids

    def setTids(self, tids):
        self._tids = tids
        return self

    @property
    def includeTid(self):
        return self._includeTid

    def setIncludeTid(self, includeTid):
        self._includeTid = includeTid
        return self


class HippoReceiverConfig(ReceiverConfig):
    def __init__(self):
        super(HippoReceiverConfig, self).__init__()
        self._controllerAddrs = None
        self._batchCount = 5

    @property
    def controllerAddrs(self):
        return self._controllerAddrs

    def setControllerAddrs(self, controllerAddrs):
        self._controllerAddrs = controllerAddrs
        return self

    @property
    def batchCount(self):
        return self._batchCount

    def setBatchCount(self, batchCount):
        self._batchCount = batchCount
        return self


def encodeUTF8(iterator):
    for x in iterator:
        if not isinstance(x, (unicode, bytes)):
            x = unicode(x)
        if isinstance(x, unicode):
            x = x.encode("utf-8")
        yield x


class DataProvider(object):
    """
    Base class for data access
    """
    def name(self):
        pass


class TDWProvider(DataProvider):
    def __init__(self, sc, user=None, passwd=None, db=None, group="tl"):
        # From tdw-spark-toolkit 3.6.0, the argument db's default value is set None,
        # but it can not be None
        assert db is not None, 'db can not be None'
        self.sc = sc
        self._tdw_provider = sc._jvm.\
            com.tencent.tdw.spark.toolkit.api.python.PythonTDWProvider(sc._jsc, user, passwd, db, group)

    def name(self):
        return "TDW-RDD"

    def table(self, tblName, priParts=None, subParts=None, use_unicode=True):
        jrdd = self._tdw_provider.table(tblName, priParts, subParts)
        return RDD(jrdd, self.sc, UTF8Deserializer(use_unicode)).map(lambda x: x.split("\01", -1))

    def saveToTable(self, rdd, tblName, priPart=None, subPart=None, overwrite=True):
        encoded = rdd.map(lambda x: "\01".join(x)).mapPartitions(encodeUTF8)
        encoded._bypass_serializer = True
        self._tdw_provider.saveToTable(encoded._jrdd.map(self.sc._jvm.BytesToString()),
                                       tblName, priPart, subPart, overwrite)

    def protobufTable(self, tblName, priParts=None, subParts=None):
        jrdd = self._tdw_provider.protobufTable(tblName, priParts, subParts)
        return RDD(jrdd, self.sc, NoOpSerializer())

    def saveToProtobufTable(self, rdd, tblName, priPart=None, subPart=None, overwrite=True):
        rdd._bypass_serializer = True
        self._tdw_provider.saveToProtobufTable(rdd._jrdd, tblName, priPart, subPart, overwrite)


class TDWSQLProvider(DataProvider):
    def __init__(self, session, user=None, passwd=None, db=None, group="tl"):
        # From tdw-spark-toolkit 3.6.0, the argument db's default value is set None,
        # but it can not be None
        assert db is not None, 'db can not be None'
        self.session = session
        self._tdw_sql_provider = session._jvm.\
            com.tencent.tdw.spark.toolkit.api.python.PythonTDWSQLProvider(session._jsparkSession, user, passwd, db, group)

    def name(self):
        return "TDW-DATAFRAME"

    def table(self, tblName, priParts=None, subParts=None):
        df = self._tdw_sql_provider.table(tblName, priParts, subParts)
        return DataFrame(df, self.session._wrapped)

    def saveToTable(self, df, tblName, priPart=None, subPart=None, overwrite=True):
        self._tdw_sql_provider.saveToTable(df._jdf, tblName, priPart, subPart, overwrite)


class TDBankProvider(DataProvider):
    def __init__(self, ssc):
        self.ssc = ssc
        self._tdbank_provider = ssc._jvm.\
            com.tencent.tdw.spark.toolkit.api.python.PythonTDBankProvider(ssc._jssc)

    def name(self):
        return "TDBANK-DSTREAM"

    def textStream(self, config, numReceiver, use_unicode=True):
        assert isinstance(config, TubeReceiverConfig) or isinstance(config, HippoReceiverConfig),\
            'config must be instantiated from TubeReceiverConfig or HippoReceiverConfig'

        jconfig = self.ssc._jvm.com.tencent.tdw.spark.toolkit.tdbank.TubeReceiverConfig().buildFrom(config.master,
                                                                                                    config.group,
                                                                                                    config.topic,
                                                                                                    config.tids,
                                                                                                    config.consumeFromMaxOffset,
                                                                                                    config.includeTid,
                                                                                                    self.ssc.sparkContext._getJavaStorageLevel(config.storageLevel)) \
            if isinstance(config, TubeReceiverConfig) else \
            self.ssc._jvm.com.tencent.tdw.spark.toolkit.tdbank.HippoReceiverConfig().buildFrom(config.controllerAddrs,
                                                                                               config.group,
                                                                                               config.topic,
                                                                                               config.batchCount,
                                                                                               config.consumeFromMaxOffset,
                                                                                               self.ssc.sparkContext._getJavaStorageLevel(config.storageLevel))
        dstream = self._tdbank_provider.textStream(jconfig, numReceiver)
        return DStream(dstream, self.ssc,  UTF8Deserializer(use_unicode))

    def bytesStream(self, config, numReceiver):
        assert isinstance(config, TubeReceiverConfig) or isinstance(config, HippoReceiverConfig), \
            'config must be instantiated from TubeReceiverConfig or HippoReceiverConfig'

        jconfig = self.ssc._jvm.com.tencent.tdw.spark.toolkit.tdbank.TubeReceiverConfig().buildFrom(config.master,
                                                                                                    config.group,
                                                                                                    config.topic,
                                                                                                    config.tids,
                                                                                                    config.consumeFromMaxOffset,
                                                                                                    config.includeTid,
                                                                                                    self.ssc.sparkContext._getJavaStorageLevel(config.storageLevel)) \
            if isinstance(config, TubeReceiverConfig) else \
            self.ssc._jvm.com.tencent.tdw.spark.toolkit.tdbank.HippoReceiverConfig().buildFrom(config.controllerAddrs,
                                                                                               config.group,
                                                                                               config.topic,
                                                                                               config.batchCount,
                                                                                               config.consumeFromMaxOffset,
                                                                                               self.ssc.sparkContext._getJavaStorageLevel(config.storageLevel))
        dstream = self._tdbank_provider.bytesStream(jconfig, numReceiver)
        return DStream(dstream, self.ssc,  NoOpSerializer())