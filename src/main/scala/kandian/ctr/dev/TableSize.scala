package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWUtil
import org.apache.spark.sql.SparkSession

object TableSize {

  def main(args: Array[String]): Unit = {
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    //println(dateStr)
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")
    val spark = SparkSession.builder().getOrCreate()

    val tdwUtil1 = new TDWUtil(user, pwd, "sng_mediaaccount_app")
    val dateList = DateUtil.getOneWeekString(dateStr)
    for (date <- dateList){
      val size1 = tdwUtil1.tableSize("rerank_train_data_kehuduan", Array("p_"+date))
      println("rerank_train_data_kehuduan " + date + " : \t"+size1)
    }

    val tdwUtil2 = new TDWUtil(user, pwd, "sng_qqkd_ml_app")
    val size2 = tdwUtil2.tableSize("simsonhe_kandian_cf_predict_data")
    println("simsonhe_kandian_cf_predict_data: "+size2)
    val size5 = tdwUtil2.tableSize("simsonhe_kandian_cf_to_predict_data")
    println("println(\"simsonhe_kandian_cf_predict_data: "+size5)

    val tDWUtil3 = new TDWUtil(user, pwd, "sng_tdbank")
    val size3 = tDWUtil3.tableSize("kd_dsl_user_potrait_online_fdt0", Array("p_"+dateStr))
    println("kd_dsl_user_potrait_online_fdt0: " + dateStr + " : \t" + size3)

    val size4 = tdwUtil1.tableSize("userprofile_click_d")
    println("userprofile_click_d: " + size4)

  }
}
