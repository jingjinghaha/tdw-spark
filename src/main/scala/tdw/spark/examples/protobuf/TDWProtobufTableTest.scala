package com.tencent.tdw.spark.examples.protobuf

import com.tencent.tdw.spark.examples.protobuf.AddressBookProtos.address_book
import com.tencent.tdw.spark.examples.protobuf.AddressBookProtos.address_book.Person
import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWProvider
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by sharkdtu
 * 2015/10/28.
 */
object TDWProtobufTableTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val cmdArgs = new GeneralArgParser(args)

    val srcTbl = cmdArgs.getStringValue("src_tbl")

    val tdw = new TDWProvider(sc, "test_user", "test_passwd", "test_db")

    val person1 = Person.newBuilder().setId(1).setName("hello").setEmail("hello@tencent.com").build()
    val person2 = Person.newBuilder().setId(2).setName("world").setEmail("world@tencent.com").build()
    val addrBook1 = address_book.newBuilder().setId(1).addPerson(person1).addPerson(person2).build()
    val addrBook2 = address_book.newBuilder().setId(2).addPerson(person1).addPerson(person2).build()

    // fisrt, write some data to your protobuf table
    val data = Seq(addrBook1.toByteArray, addrBook2.toByteArray)
    val len1 = data.map(_.length).sum
    tdw.saveToProtobufTable(sc.makeRDD(data), srcTbl)

    // secondly, read from the protobuf table
    val data2 = tdw.protobufTable(srcTbl).cache()
    val len2 = data2.collect().map(_.length).sum

    // check data length
    println(s"len1: $len1")
    println(s"len2: $len2")

    // check data content
    data2.map { bytes =>
      // address_book.parseFrom(bytes).getId
      address_book.newBuilder().mergeFrom(bytes).build().getId
    }.collect().foreach(x => println(s"addressbook id: $x"))
  }
}
