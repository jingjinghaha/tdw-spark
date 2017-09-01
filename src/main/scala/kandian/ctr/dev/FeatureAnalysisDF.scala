package com.tencent.kandian.ctr.dev

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.sql.SparkSession

/**
  * Created by franniewu on 2017/7/20.
  */
object FeatureAnalysisDF {

  def main(args: Array[String]): Unit = {
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)

    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()
    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val data = tdw.table("simsonhe_kandian_train_data", Seq("p_"+dateStr))

    val Array(data_partition, data_left) = data.filter(
      data("date_key").isNotNull &&
        data("hour").isNotNull &&
        data("city_level").isNotNull &&
        data("stay_time").isNotNull &&
        data("title_length").isNotNull &&
        data("cover_pic").isNotNull &&
        data("quality_score").isNotNull &&
        data("account_score").isNotNull &&
        data("attract_point").isNotNull &&
        data("read_time").isNotNull &&
        data("read_complete").isNotNull &&
        data("title_emotion").isNotNull &&
        data("title_attract").isNotNull &&
        data("channel_click_rate").isNotNull &&
        data("channel_like_rate").isNotNull &&
        data("channel_comment_rate").isNotNull &&
        data("channel_id").isNotNull &&
        data("algorithm_rate").isNotNull &&
        data("algorithm_id").isNotNull &&
        data("uin").isNotNull &&
        data("article_id").isNotNull &&
        data("di_score").isNotNull &&
        data("di_usermodel_score").isNotNull &&
        data("di_tag_score").isNotNull &&
        data("di_hqtag_score").isNotNull &&
        data("account_read_score").isNotNull &&
        data("account_click_score").isNotNull &&
        data("account_like_score").isNotNull &&
        data("account_comment_score").isNotNull &&
        data("account_health_score").isNotNull &&
        data("account_res_score").isNotNull).repartition(10000).randomSplit(Array(0.5, 0.5))

    val article_info = tdw.table("rerank_article_tag", DateUtil.getOneWeek(dateStr)).toDF("today_date", "cms_article_id","id_2","id_3","ent","women","tech","astro","sports","life","comic","auto","finance","cul","funny","emotion","travel","food","house","mil","health","science_level1","edu","baby","game","houseliving","social","inspiration","politics","beauty","other","world","agriculture","history","creativity","pet","basketball_level1","football_level1","astro_shengxiao","astro_mingli","astro_psytest","astro_xingzuo","astro_fengshui","auto_yongche","auto_ceping","auto_chanye","auto_daogou","auto_diandongche","auto_xinche","auto_takecare","auto_suv","baby_yunchan","baby_qinzichengchang","baby_autism","baby_prepare","career_business","career_common","career_hr","career_market","career_venture","comic_cosplay","cul_dushu","cul_wenwu","cul_sheying","cul_yishu","cul_building","cul_collect","cul_painting","cul_shufa","cul_magic","cul_instrument","cul_wenxue","cul_guoxue","cul_minsu","cul_shige","cul_shougongyi","digital_phone","digital_hardware","digital_industry","digital_iphone","digital_chip","digital_chuandaishebei","digital_jiadian","digital_kuwan","digital_pad","digital_pc","digital_tv","edu_abroad","edu_college","edu_primary","edu_skills","edu_gaokao","edu_kaoyan","edu_gongwuyuan","edu_english","edu_gmat","edu_ielts","edu_sat","edu_gre","edu_mba","edu_accountexam","edu_lawexam","edu_online","edu_oversea","edu_toelf","emotion_inlove","emotion_marriage","emotion_affair","film","tvplay","show","drama","music","star","ent_hanyu","ent_hongkong","ent_janpan","ent_western","ent_americantv","ent_china","ent_englishtv","ent_quyi","ent_talkshow","ent_xiangsheng","ent_xiaoping","bank","bond","frgexchange","fund","futures","insurance","interfinance","inverstment","macroeco","mngmoney","stock","finance_bit","finance_business","finance_corporation","finance_famous","finance_internation","finance_xintuo","finance_energy","finance_metal","food_zhongcan","food_caipu","food_ribenliaoli","food_guide","food_pengrenjiqiao","food_xiaochi","funny_pic","funny_story","game_dianjing","game_industry","game_pandian","game_shouyou","game_wangyou","game_youxiji","health_jianfei","health_yangsheng","health_zhongyi","health_kouqianghuli","health_fuke","health_aizheng","health_shipinganquan","health_chuanranbing","health_pifubing","health_psychology","health_control","health_old","health_sex","health_man","health_woman","health_care","health_baby","health_products","health_food","history_kangrizhanzheng","history_ancient","history_mingqing","history_minguo","history_party","history_wenge","history_zhiqing","history_gaigekaifang","history_world","history_wwi","history_wwii","history_jianguochuqi","history_kangmeiyuanchao","history_yuanyue","history_sanfanwufan","history_foreign","house_chuweidianqi","house_duorouzhiwu","house_jiaju","house_zhuangxiu","house_lease","house_oversea","house_pm","house_sell","houseliving_handcraft","houseliving_jiadian","houseliving_plant","lifestyle_coffee","lifestyle_fishing","lifestyle_liqueur","lifestyle_martial","lifestyle_paobu","lifestyle_tea","lifestyle_yoga","lifestyle_aerobics","lifestyle_dance","lifestyle_taiji","lifestyle_wine","lifestyle_fitness","mil_zhongguojunqing","mil_hangkonghangtian","mil_war","mil_weapon","mil_internation","photography_skills","photography_tool","politics_international","politics_domestic","religion_buddhism","religion_christian","science_space","science_popular","social_qiwendieshi","social_jihuashengyu","social_huanjingbaohu","social_community","social_mingsheng","social_cishan","social_offense","social_traffic","athletics","badmiton","basketball","chess","pingpang","swimming","tennis","volleyball","sports_cba","sports_nba","sports_boji","sports_dejia","golf","sports_outdoor","sports_huwaiyundong","sports_saiche","sports_taiqiu","sports_xijia","sports_yijia","sports_yingchao","sports_zhongchao","football","sports_ski","sports_zhongguozuqiu","internet","intelligent","networksecurity","software","science","tech_vr","tech_artifical","tech_communication","tech_famous","tech_iot","tech_megadata","tech_o2o","tech_retail","tech_sns","tech_uav","tech_venture","tech_yun","travel_africa","travel_america","travel_demostic","travel_europa","travel_hm","travel_information","travel_jk","travel_soucheastasia","women_meiti","women_zhubao","women_shizhuang","women_hair","women_luxury","women_wanbiao","women_makeup","women_skincare","women_beauty","women_man","women_meiti_xiangshui","women_meiti_meijia","women_skincare_fangshai","women_skincare_bsbs","women_skincare_xiufu","women_skincare_qingjie","women_makeup_lianzhuang","women_makeup_chunzhuang","women_makeup_yanzhuang","women_makeup_meimao","game_cf","game_baoxue","game_dianjing_ent","game_dianjing_info","game_dnf","game_lol_ent","game_lol_gonglue","game_lol_neirong","game_lol_yingxiong","game_wzry_ent","game_wzry_gonglue","game_wzry_neirong","game_wzry_yxpandian","game_yingyangshi","game_zhubo","comic_dongmanpandian","comic_dongmanxuanfa","comic_gaoxiaomanhua","comic_haizeiwang","comic_huoyingrenzhe","comic_kenan","comic_kongbumanhua","comic_lianzaimanhua","comic_longzhu","comic_nannvshengmanhua","comic_neihanmanhua","comic_othermanhua","comic_seximanhua","comic_shengyou","comic_wuyemanhua","comic_xieemanhua","comic_xuanyimanhua","h1","h2","h3","h4","h5","h6","h7","h8","h9","h10","h11","h12","h13","h14","h15","h16","h17","h18","h19","h20","hh1","h2h","hh3","hh4","hh5","hh6","hh7","hh8","hh9","hh10","h1h1","hh12","hh13","hh14","hh15",
      "hh16","hh17","h1h8","hh19","hh20","hw1","hw2","hw3","hw4","hw5","hw6","hw7","hw8","hw9","hw10","hw11","hw12","hw13","hw14","h1w5","hw16","hw17","h1w8","hw19","h2w0","wh1","wh2","wh3","wh4","wh5","wh6","wh7","wh8","wh9","wh10","wh11","wh12",
      "wh13","wh14","wh15","wh16","wh17","wh18","wh19","wh20")

    val userProfileTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val userProfileData = userProfileTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + dateStr))
      .select("tdbank_imp_date", "uin", "potrait").toDF("tdbank_imp_date", "uin2", "potrait")//.cache()
    val userProfileDF = userProfileData.dropDuplicates("uin2").repartition(10000)

    val filteredProfile = data_partition.join(userProfileDF, data_partition("uin") === userProfileDF("uin2"),"inner")//.repartition(8000)//.cache()

    val filteredArticle = filteredProfile.join(article_info, article_info("cms_article_id") === filteredProfile("article_id")).repartition(10000)//.cache()

    val df_active_level = filteredArticle.select("label","avtive_level")

    val df_active_level_part0 = df_active_level.filter(df_active_level("avtive_level").lt(0.5))
    val df_active_level_part1 = df_active_level.filter(df_active_level("avtive_level").gt(0.5) && df_active_level("avtive_level").lt(1) || df_active_level("avtive_level").equalTo(0.5))
    val df_active_level_part2 = df_active_level.filter(df_active_level("avtive_level").gt(1) && df_active_level("avtive_level").lt(2) || df_active_level("avtive_level").equalTo(1))
    val df_active_level_part3 = df_active_level.filter(df_active_level("avtive_level").gt(2) && df_active_level("avtive_level").lt(3) || df_active_level("avtive_level").equalTo(2))
    val df_active_level_part4 = df_active_level.filter(df_active_level("avtive_level").gt(3) && df_active_level("avtive_level").lt(4) || df_active_level("avtive_level").equalTo(3))
    val df_active_level_part5 = df_active_level.filter(df_active_level("avtive_level").gt(4) && df_active_level("avtive_level").lt(5) || df_active_level("avtive_level").equalTo(4))
    val df_active_level_part6 = df_active_level.filter(df_active_level("avtive_level").gt(5) && df_active_level("avtive_level").lt(6) || df_active_level("avtive_level").equalTo(5))
    val df_active_level_part7 = df_active_level.filter(df_active_level("avtive_level").gt(6) && df_active_level("avtive_level").lt(7) || df_active_level("avtive_level").equalTo(6))
    val df_active_level_part8 = df_active_level.filter(df_active_level("avtive_level").gt(7) && df_active_level("avtive_level").lt(8) || df_active_level("avtive_level").equalTo(7))
    val df_active_level_part9 = df_active_level.filter(df_active_level("avtive_level").gt(9) || df_active_level("avtive_level").equalTo(9))

    val df_active_level_part0_pos = df_active_level_part0.filter(df_active_level_part0("label")===1)
    val df_active_level_part0_neg = df_active_level_part0.filter(df_active_level_part0("label")===0)
    val df_active_level_part1_pos = df_active_level_part1.filter(df_active_level_part1("label")===1)
    val df_active_level_part1_neg = df_active_level_part1.filter(df_active_level_part1("label")===0)
    val df_active_level_part2_pos = df_active_level_part2.filter(df_active_level_part2("label")===1)
    val df_active_level_part2_neg = df_active_level_part2.filter(df_active_level_part2("label")===0)
    val df_active_level_part3_pos = df_active_level_part3.filter(df_active_level_part3("label")===1)
    val df_active_level_part3_neg = df_active_level_part3.filter(df_active_level_part3("label")===0)
    val df_active_level_part4_pos = df_active_level_part4.filter(df_active_level_part4("label")===1)
    val df_active_level_part4_neg = df_active_level_part4.filter(df_active_level_part4("label")===0)
    val df_active_level_part5_pos = df_active_level_part5.filter(df_active_level_part5("label")===1)
    val df_active_level_part5_neg = df_active_level_part5.filter(df_active_level_part5("label")===0)
    val df_active_level_part6_pos = df_active_level_part6.filter(df_active_level_part6("label")===1)
    val df_active_level_part6_neg = df_active_level_part6.filter(df_active_level_part6("label")===0)
    val df_active_level_part7_pos = df_active_level_part7.filter(df_active_level_part7("label")===1)
    val df_active_level_part7_neg = df_active_level_part7.filter(df_active_level_part7("label")===0)
    val df_active_level_part8_pos = df_active_level_part8.filter(df_active_level_part8("label")===1)
    val df_active_level_part8_neg = df_active_level_part8.filter(df_active_level_part8("label")===0)
    val df_active_level_part9_pos = df_active_level_part9.filter(df_active_level_part9("label")===1)
    val df_active_level_part9_neg = df_active_level_part9.filter(df_active_level_part9("label")===0)

    println("df_active_level_part0 in range\t [0 0.5)\t" + df_active_level_part0_pos.count() + "\t" + df_active_level_part0_neg.count())
    println("df_active_level_part1 in range\t [0.5 1)\t" + df_active_level_part1_pos.count() + "\t" + df_active_level_part1_neg.count())
    println("df_active_level_part2 in range\t [1 2)\t" + df_active_level_part2_pos.count() + "\t" + df_active_level_part2_neg.count())
    println("df_active_level_part3 in range\t [2 3)\t" + df_active_level_part3_pos.count() + "\t" + df_active_level_part3_neg.count())
    println("df_active_level_part4 in range\t [3 4)\t" + df_active_level_part4_pos.count() + "\t" + df_active_level_part4_neg.count())
    println("df_active_level_part5 in range\t [4 5)\t" + df_active_level_part5_pos.count() + "\t" + df_active_level_part5_neg.count())
    println("df_active_level_part6 in range\t [5 6)\t" + df_active_level_part6_pos.count() + "\t" + df_active_level_part6_neg.count())
    println("df_active_level_part7 in range\t [6 7)\t" + df_active_level_part7_pos.count() + "\t" + df_active_level_part7_neg.count())
    println("df_active_level_part8 in range\t [7 8)\t" + df_active_level_part8_pos.count() + "\t" + df_active_level_part8_neg.count())
    println("df_active_level_part9 in range\t [9 )\t" + df_active_level_part9_pos.count() + "\t" + df_active_level_part9_neg.count())

  }
}
