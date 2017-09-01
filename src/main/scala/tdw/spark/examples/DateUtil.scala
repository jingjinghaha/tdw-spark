package com.tencent.tdw.spark.examples

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ArrayBuffer

/**
  * Created by simsonhe on 2017/1/11.
  */
object DateUtil {

  def oneMonthPartSeq(yesterday: String): Seq[String] = {

    var dateSeq: Seq[String] = Seq();

    val df = new SimpleDateFormat("yyyyMMdd")

    val date = df.parse(yesterday)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)

    for (dayBefore <- 1 to 30) {
      calendar.add(Calendar.DAY_OF_MONTH, -dayBefore)
      dateSeq = dateSeq.:+("p_" + df.format(calendar.getTime))
    }

    return dateSeq
  }

  def oneMonthPartSeqIncludeToday(yesterday: String): Seq[String] = {

    var dateSeq: Seq[String] = Seq();

    val df = new SimpleDateFormat("yyyyMMdd")

    val date = df.parse(yesterday)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)

    for (dayBefore <- 0 to 30) {
      calendar.add(Calendar.DAY_OF_MONTH, -dayBefore)
      dateSeq = dateSeq.:+("p_" + df.format(calendar.getTime))
    }

    return dateSeq
  }

  def getTodayDate(yesterday: String): Seq[String] = {
    var dateSeq: Seq[String] = Seq();
    val df = new SimpleDateFormat("yyyyMMdd")

    val yDate = df.parse(yesterday)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(yDate)

    calendar.add(Calendar.DAY_OF_MONTH, +1)
    val todayDate = calendar.getTime

    val dateStr = df.format(todayDate)
    dateSeq = dateSeq.:+("p_" + dateStr)
    return dateSeq
  }

  def getWeekDateIinterval(yesterday: String): Seq[String] = {

    var dateSeq: Seq[String] = Seq();

    val df = new SimpleDateFormat("yyyyMMdd")

    val date = df.parse(yesterday)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)

    for (dayBefore <- 0 to 6) {
      calendar.add(Calendar.DAY_OF_MONTH, -dayBefore)

      val dateStr = df.format(calendar.getTime)

      for (timeH <- 0 to 23) {
        if (timeH < 10) {
          dateSeq = dateSeq.:+("p_" + dateStr + "0" + timeH)
        }
        else {
          dateSeq = dateSeq.:+("p_" + dateStr + timeH)
        }
      }
    }

    return dateSeq
  }

  def getOneDayIinterval(yesterday: String): Seq[String] = {

    var dateSeq: Seq[String] = Seq();

    for (timeH <- 0 to 23) {
      if (timeH < 10) {
        dateSeq = dateSeq.:+("p_" + yesterday + "0" + timeH)
      }
      else {
        dateSeq = dateSeq.:+("p_" + yesterday + timeH)
      }

    }

    return dateSeq
  }

  def getThreeDateIinterval(yesterday: String): Seq[String] = {

    var dateSeq: Seq[String] = Seq();

    val df = new SimpleDateFormat("yyyyMMdd")

    val date = df.parse(yesterday)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)

    for (dayBefore <- 0 to 2) {
      calendar.add(Calendar.DAY_OF_MONTH, -dayBefore)

      val dateStr = df.format(calendar.getTime)

      for (timeH <- 0 to 23) {
        if (timeH < 10) {
          dateSeq = dateSeq.:+("p_" + dateStr + "0" + timeH)
        }
        else {
          dateSeq = dateSeq.:+("p_" + dateStr + timeH)
        }
      }
    }

    return dateSeq
  }

  def getThreeDate(yesterday: String): Seq[String] = {

    var dateSeq: Seq[String] = Seq();

    val df = new SimpleDateFormat("yyyyMMdd")

    val date = df.parse(yesterday)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)

    for (dayBefore <- 0 to 2) {
      calendar.add(Calendar.DAY_OF_MONTH, -dayBefore)

      val dateStr = df.format(calendar.getTime)
      dateSeq = dateSeq.:+("p_" + dateStr)
    }

    return dateSeq
  }

  def getOneDaySeq(argDate:String): Seq[String] ={
    val hours = Array("00","01","02","03","04","05","06","07","8","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23")
    val results = new ArrayBuffer[String]()
    for(hour <- hours) {
      results += ("p_"+argDate+hour)
    }
    results
  }

  def getInfoPartSeq(argDate: String): Seq[String] = {
    val df = new SimpleDateFormat("yyyyMMdd")
    val dfs = new SimpleDateFormat("yyyyMM")

    val date = df.parse(argDate)

    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_MONTH, -10)
    calendar.add(Calendar.MONTH, -1)

    val newDate = calendar.getTime
    val newDateStr = "p_" + dfs.format(newDate)
    val dateSeq = Seq(newDateStr)
    dateSeq
  }

}
