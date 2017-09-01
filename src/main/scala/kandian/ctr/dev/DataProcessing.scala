package com.tencent.kandian.ctr.dev

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.tencent.tdw.spark.examples.util.GeneralArgParser
import com.tencent.tdw.spark.toolkit.tdw.TDWSQLProvider
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.{asc, col, udf, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.parsing.json.JSON

/**
  * Created by franniewu on 2017/5/3.
  */
object DataProcessing {

  class ElementContainer[T] { def unapply(element:Any):Option[T] = Some(element.asInstanceOf[T]) }
  object MapContainer extends ElementContainer[Map[String, Any]]
  object ListContainer extends ElementContainer[List[Any]]
  object StringContainer extends ElementContainer[String]
  object DoubleContainer extends ElementContainer[Double]
  object IntContainer extends ElementContainer[Int]

  def main(args: Array[String]): Unit = {
    val cmdArgs = new GeneralArgParser(args)
    val dateStr: String = cmdArgs.getStringValue("dateStr")
    println(dateStr)
    val user: String = cmdArgs.getStringValue("user")
    val pwd: String = cmdArgs.getStringValue("pwd")

    val spark = SparkSession.builder().getOrCreate()

    val tdw = new TDWSQLProvider(spark, user, pwd, "sng_mediaaccount_app")
    val data = tdw.table("simsonhe_kandian_train_data", Seq("p_"+dateStr))
    val filtered_data = data.filter(data("gender").equalTo(2) && data("age").gt(15) && data("age").lt(19) && data("avtive_level").gt(10))
    filtered_data.show(5)

    val userProfileTdw = new TDWSQLProvider(spark, user, pwd, "sng_tdbank")
    val userProfileData = userProfileTdw.table("kd_dsl_user_potrait_online_fdt0", Seq("p_" + dateStr))
      .select("tdbank_imp_date", "uin", "potrait").toDF("tdbank_imp_date", "uin2", "potrait").cache()
    userProfileData.show(5)
    // it is unsure whether the first row or the last row is kept.
    val userProfileRows = userProfileData.rdd.map(r => (r.get(1), r)).reduceByKey((r0, r1) => r0).map(r => r._2).map(r => Row.fromSeq(r.toSeq))
    val userTagSchema = StructType(userProfileData.schema.fields)
    val userProfileDF = spark.createDataFrame(userProfileRows, userTagSchema)
    userProfileDF.show(5)

    val filteredProfile = filtered_data.join(userProfileDF, filtered_data("uin") === userProfileDF("uin2"),"inner").cache()
    println("the number of instance in train data: " + filtered_data.count())
    println("the number of instance after join user profile: " + filteredProfile.count())

    val transData = transFeatureMapping(filteredProfile, dateStr)
    transData.show(5)
    spark.stop()
  }

  def transFeatureMapping(originalData: DataFrame, dateStr: String): DataFrame = {

    // "date_key", "hour", "gender", "age", "city_level",
    // "login_freq" no-use, "active_level", "title_length",
    // "cover_pic", "quality_score", "account_score", "attract_point", "read_time",
    // "read_complete", "title_emotion", "channel_click_rate",
    // "channel_like_rate", "channel_comment_rate", "channel_id", "algorithm_rate",
    // "algorithm_id", "label", "uin", "article_id", "is_holiday","title_ellipsis",
    // "di_score", "di_usermodel_score","di_hot","di_tag_score","network",
    // "di_hqtag_score","account_read_score","account_click_score","account_like_score",
    // "account_comment_score","account_health_score","account_res_score"

    //hour, sparse to 24 dimensions
    val hourVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(24, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(24, Array(), Array())
      }
    }
    val hourFunc = udf(hourVector)

    //age, sparse to 60 dimensions
    val ageVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(60, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(60, Array(), Array())
      }
    }
    val ageFunc = udf(ageVector)

    //cityLevel, sparse to 6 dimensions
    val cityVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(6, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(6, Array(), Array())
      }
    }
    val cityFunc = udf(cityVector)

    //title length, sparse to 65 dimensions
    val titleLenVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(65, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(65, Array(), Array())
      }
    }
    val titleLenFunc = udf(titleLenVector)

    //cover picture, sparse to 3 dimensions
    val coverPicVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(3, Array(getCoverPicMap(0).apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(3, Array(), Array())
      }
    }
    val coverPicFunc = udf(coverPicVector)

    //channel, sparse to 60 dimensions
    val channelToIndexMap = getChannelToIndexMap(0)
    val channelVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(51, Array(channelToIndexMap.apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(51, Array(), Array())
      }
    }
    val channelFunc = udf(channelVector)

    //algorithm, sparse to 200 dimensions
    val algorithmToIndexMap = getAlgorithmToIndexMap(0)
    val algorithmVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(200, Array(algorithmToIndexMap.apply(arg.toInt)), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(200, Array(), Array())
      }
    }
    val algorithmFunc = udf(algorithmVector)

    // the day of week, sparse to 7 dimensions
    val dayVector: (Long => Vector) = (dateStr: Long) => {
      try {
        val df = new SimpleDateFormat("yyyyMMdd")
        val date = df.parse(dateStr.toString)
        val calendar: Calendar = Calendar.getInstance
        calendar.setTime(date)
        val dayIndex = calendar.get(Calendar.DAY_OF_WEEK)
        Vectors.sparse(7, Array(dayIndex - 1), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(7, Array(), Array())
      }
    }
    val dayFunc = udf(dayVector)

    //title ellipsis, sparse to 2 dimensions
    val titleEllipsisVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(2, Array(arg.toInt), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(2, Array(), Array())
      }
    }
    val titleEllipsisFunc = udf(titleEllipsisVector)

    ///******************************************************************
    // the tag container must be double
    val get1stChannelArray: (String => Vector) = (arg: String) => {
      try {
        //println(arg)
        val userTagList =
          for {
            Some(MapContainer(userMap)) <- List(JSON.parseFull(arg))
            ListContainer(tagList) = userMap("channel_model")
            MapContainer(tagMap) <- tagList
            DoubleContainer(tagIdx) = tagMap("id")
            DoubleContainer(tagWeight) = tagMap("weight")
            userTagIdxNew = get1stChannelMap(0).apply(tagIdx.toInt)
          } yield {
            (userTagIdxNew, tagWeight)
          }
        //print(userTagList.toString())
        Vectors.sparse(46, userTagList)
      } catch {
        case ex: Exception => {
          //println("xxxxxxxxxxxxxx")
          Vectors.sparse(46, Array(), Array())
        }
      }
    }
    val get1stChannelFunc = udf(get1stChannelArray)

    val get2ndChannelArray: (String => Vector) = (arg: String) => {
      try {
        val userTagList =
          for {
            Some(MapContainer(userMap)) <- List(JSON.parseFull(arg))
            ListContainer(tagList) = userMap("channel_model_beta")
            MapContainer(tagMap) <- tagList
            DoubleContainer(tagIdx) = tagMap("id")
            DoubleContainer(tagWeight) = tagMap("weight")
            userTagIdxNew = get2ndChannelMap(0).apply(tagIdx.toInt)
          } yield {
            (userTagIdxNew, tagWeight)
          }
        //print(userTagList.toString())
        Vectors.sparse(350, userTagList)
      } catch {
        case ex: Exception => Vectors.sparse(350, Array(), Array())
      }
    }
    val get2ndChannelFunc = udf(get2ndChannelArray)
    //*********************************************************************/

    val final_output = originalData.withColumn("hour_vec", hourFunc(col("hour")))
      .withColumn("age_vec", ageFunc(col("age")))
      .withColumn("city_level_vec", cityFunc(col("city_level")))
      .withColumn("title_length_vec", titleLenFunc(col("title_length")))
      .withColumn("cover_pic_vec", coverPicFunc(col("cover_pic")))
      .withColumn("channel_id_vec", channelFunc(col("channel_id")))
      .withColumn("algorithm_id_vec", algorithmFunc(col("algorithm_id")))
      .withColumn("day_index_vec", dayFunc(col("date_key")))
      .withColumn("title_ellipsis_vec", titleEllipsisFunc(col("title_ellipsis")))
      .withColumn("1stWeight", get1stChannelFunc(col("potrait")))
      .withColumn("2ndWeight", get2ndChannelFunc(col("potrait")))//.repartition(8000)//.cache()

    //    "hour_vec", "gender_vec", "age_vec",
    //    "city_level_vec", "title_length_vec", "cover_pic_vec",
    //    "title_emotion_vec", "channel_id_vec", "algorithm_id_vec",
    //    "is_holiday_vec", "title_ellipsis_vec", "di_hot_vec",
    //    "network_vec", "active_level", "quality_score", "account_score",
    //    "attract_point", "read_time", "read_complete", "channel_click_rate",
    //    "channel_like_rate", "channel_comment_rate", "algorithm_rate",
    //    "di_score", "di_usermodel_score", "di_tag_score", "network", "di_hqtag_score"
    //    "account_read_score","account_click_score","account_like_score",
    //    "account_comment_score","account_health_score","account_res_score"
    val assemblerOutput = new VectorAssembler()
      .setInputCols(Array("hour_vec","age_vec",
        "title_length_vec", "cover_pic_vec",
        "channel_id_vec", "algorithm_id_vec",
        "day_index_vec", "title_ellipsis_vec",
        "avtive_level", "quality_score", "account_score",
        "read_time", "read_complete", "channel_click_rate",
        "channel_like_rate", "channel_comment_rate", "algorithm_rate",
        //"di_score", "di_usermodel_score", "di_tag_score", "di_hqtag_score",
        "account_read_score","account_click_score","account_like_score",
        "account_comment_score","account_health_score","account_res_score","1stWeight",
        "2ndWeight","ent", "women", "tech",
        "astro", "sports", "life", "comic", "auto", "finance", "cul", "funny", "emotion", "travel", "food", "house", "mil",
        "health", "science_level1", "edu", "baby", "game", "houseliving", "social", "inspiration", "politics", "beauty",
        "other", "world", "agriculture", "history", "creativity", "pet", "basketball_level1", "football_level1",
        "astro_shengxiao", "astro_mingli", "astro_psytest", "astro_xingzuo", "astro_fengshui", "auto_yongche", "auto_ceping",
        "auto_chanye", "auto_daogou", "auto_diandongche", "auto_xinche", "auto_takecare", "auto_suv", "baby_yunchan",
        "baby_qinzichengchang", "baby_autism", "baby_prepare", "career_business", "career_common", "career_hr", "career_market",
        "career_venture", "comic_cosplay", "cul_dushu", "cul_wenwu", "cul_sheying", "cul_yishu", "cul_building", "cul_collect",
        "cul_painting", "cul_shufa", "cul_magic", "cul_instrument", "cul_wenxue", "cul_guoxue", "cul_minsu", "cul_shige",
        "cul_shougongyi", "digital_phone", "digital_hardware", "digital_industry", "digital_iphone", "digital_chip",
        "digital_chuandaishebei", "digital_jiadian", "digital_kuwan", "digital_pad", "digital_pc", "digital_tv", "edu_abroad",
        "edu_college", "edu_primary", "edu_skills", "edu_gaokao", "edu_kaoyan", "edu_gongwuyuan", "edu_english", "edu_gmat", "edu_ielts",
        "edu_sat", "edu_gre", "edu_mba", "edu_accountexam", "edu_lawexam", "edu_online", "edu_oversea", "edu_toelf", "emotion_inlove",
        "emotion_marriage", "emotion_affair", "film", "tvplay", "show", "drama", "music", "star", "ent_hanyu", "ent_hongkong",
        "ent_janpan", "ent_western", "ent_americantv", "ent_china", "ent_englishtv", "ent_quyi", "ent_talkshow", "ent_xiangsheng",
        "ent_xiaoping", "bank", "bond", "frgexchange", "fund", "futures", "insurance", "interfinance", "inverstment",
        "macroeco", "mngmoney", "stock", "finance_bit", "finance_business", "finance_corporation", "finance_famous", "finance_internation",
        "finance_xintuo", "finance_energy", "finance_metal", "food_zhongcan", "food_caipu", "food_ribenliaoli", "food_guide",
        "food_pengrenjiqiao", "food_xiaochi", "funny_pic", "funny_story", "game_dianjing", "game_industry", "game_pandian",
        "game_shouyou", "game_wangyou", "game_youxiji", "health_jianfei", "health_yangsheng", "health_zhongyi", "health_kouqianghuli",
        "health_fuke", "health_aizheng", "health_shipinganquan", "health_chuanranbing", "health_pifubing", "health_psychology",
        "health_control", "health_old", "health_sex", "health_man", "health_woman", "health_care", "health_baby", "health_products",
        "health_food", "history_kangrizhanzheng", "history_ancient", "history_mingqing", "history_minguo", "history_party",
        "history_wenge", "history_zhiqing", "history_gaigekaifang", "history_world", "history_wwi", "history_wwii",
        "history_jianguochuqi", "history_kangmeiyuanchao", "history_yuanyue", "history_sanfanwufan", "history_foreign",
        "house_chuweidianqi", "house_duorouzhiwu", "house_jiaju", "house_zhuangxiu", "house_lease", "house_oversea", "house_pm",
        "house_sell", "houseliving_handcraft", "houseliving_jiadian", "houseliving_plant", "lifestyle_coffee", "lifestyle_fishing",
        "lifestyle_liqueur", "lifestyle_martial", "lifestyle_paobu", "lifestyle_tea", "lifestyle_yoga", "lifestyle_aerobics", "lifestyle_dance",
        "lifestyle_taiji", "lifestyle_wine", "lifestyle_fitness", "mil_zhongguojunqing", "mil_hangkonghangtian", "mil_war",
        "mil_weapon", "mil_internation", "photography_skills", "photography_tool", "politics_international", "politics_domestic",
        "religion_buddhism", "religion_christian", "science_space", "science_popular", "social_qiwendieshi", "social_jihuashengyu",
        "social_huanjingbaohu", "social_community", "social_mingsheng", "social_cishan", "social_offense", "social_traffic",
        "athletics", "badmiton", "basketball", "chess", "pingpang", "swimming", "tennis", "volleyball", "sports_cba",
        "sports_nba", "sports_boji", "sports_dejia", "golf", "sports_outdoor", "sports_huwaiyundong", "sports_saiche",
        "sports_taiqiu", "sports_xijia", "sports_yijia", "sports_yingchao", "sports_zhongchao", "football", "sports_ski",
        "sports_zhongguozuqiu", "internet", "intelligent", "networksecurity", "software", "science", "tech_vr", "tech_artifical",
        "tech_communication", "tech_famous", "tech_iot", "tech_megadata", "tech_o2o", "tech_retail", "tech_sns", "tech_uav",
        "tech_venture", "tech_yun", "travel_africa", "travel_america", "travel_demostic", "travel_europa", "travel_hm", "travel_information",
        "travel_jk", "travel_soucheastasia", "women_meiti", "women_zhubao", "women_shizhuang", "women_hair", "women_luxury",
        "women_wanbiao", "women_makeup", "women_skincare", "women_beauty", "women_man", "women_meiti_xiangshui", "women_meiti_meijia",
        "women_skincare_fangshai", "women_skincare_bsbs", "women_skincare_xiufu", "women_skincare_qingjie", "women_makeup_lianzhuang",
        "women_makeup_chunzhuang", "women_makeup_yanzhuang", "women_makeup_meimao", "game_cf", "game_baoxue", "game_dianjing_ent",
        "game_dianjing_info", "game_dnf", "game_lol_ent", "game_lol_gonglue", "game_lol_neirong", "game_lol_yingxiong", "game_wzry_ent",
        "game_wzry_gonglue", "game_wzry_neirong", "game_wzry_yxpandian", "game_yingyangshi", "game_zhubo", "comic_dongmanpandian",
        "comic_dongmanxuanfa", "comic_gaoxiaomanhua", "comic_haizeiwang", "comic_huoyingrenzhe", "comic_kenan", "comic_kongbumanhua",
        "comic_lianzaimanhua", "comic_longzhu", "comic_nannvshengmanhua", "comic_neihanmanhua", "comic_othermanhua", "comic_seximanhua",
        "comic_shengyou", "comic_wuyemanhua", "comic_xieemanhua", "comic_xuanyimanhua"))
      .setOutputCol("features")
      .transform(final_output).select("article_id","uin","label","features")//.cache()

    assemblerOutput

  }

  def getAlgorithmToIndexMap(int: Int): Map[Long,Int] = {
    val spark = SparkSession.builder().getOrCreate()
    val props = new Properties()
    props.setProperty("user", "umqq2")
    props.setProperty("password", "pmqq_db")
    val df = spark.read.jdbc("jdbc:mysql://10.198.30.62:3392/MQQReading", "AlgorithmInfoNeiShou", props)
    val algorithmIds = df.select("AlgorithmID")
    val algorithmIds2 = algorithmIds.orderBy(asc("AlgorithmID"))

    var map: Map[Long, Int] = Map()

    val arr = algorithmIds2.collect()

    var index = 0
    for (algorithmId <- arr) {
      println(algorithmId.toString())
      map = map + (algorithmId.getLong(0) -> index)
      index = index + 1
    }
    map
  }

  def getChannelToIndexMap(int: Int): Map[Int,Int] = {

    val map: Map[Int,Int] = Map(
      56 -> 0,
      73 -> 1,
      421 -> 2,
      500 -> 3,
      501 -> 4,
      547 -> 5,
      635 -> 6,
      658 -> 7,
      867 -> 8,
      1100 -> 9,
      1166 -> 10,
      1314 -> 11,
      2573 -> 12,
      2685 -> 13,
      2979 -> 14,
      3076 -> 15,
      3689 -> 16,
      3817 -> 17,
      5293 -> 18,
      5401 -> 19,
      5729 -> 20,
      5742 -> 21,
      7900 -> 22,
      11261 -> 23,
      11626 -> 24,
      12236 -> 25,
      12709 -> 26,
      14162 -> 27,
      19085 -> 28,
      53352 -> 29,
      506879 -> 30,
      552432 -> 31,
      552433 -> 32,
      552434 -> 33,
      552436 -> 34,
      552437 -> 35,
      552438 -> 36,
      552440 -> 37,
      552441 -> 38,
      552450 -> 39,
      552452 -> 40,
      552667 -> 41,
      552912 -> 42,
      553416 -> 43,
      553665 -> 44,
      554355 -> 45,
      554611 -> 46,
      557276 -> 47,
      559608 -> 48,
      562462 -> 49,
      649551 -> 50
    )
    map
  }

  def getCoverPicMap(int: Int): Map[Int,Int] = {

    val map: Map[Int,Int] = Map(
      0 -> 0,
      1 -> 1,
      3 -> 2
    )
    map
  }
  def get1stChannelMap(int:Int): Map[Int, Int] = {
    val map: Map[Int,Int] = Map(
      0	->	0	,
      1	->	1	,
      2	->	2	,
      3	->	3	,
      4	->	4	,
      5	->	5	,
      6	->	6	,
      7	->	7	,
      8	->	8	,
      9	->	9	,
      10	->	10	,
      11	->	11	,
      12	->	12	,
      13	->	13	,
      14	->	14	,
      15	->	15	,
      16	->	16	,
      17	->	17	,
      18	->	18	,
      19	->	19	,
      20	->	20	,
      21	->	21	,
      22	->	22	,
      23	->	23	,
      24	->	24	,
      26	->	25	,
      27	->	26	,
      28	->	27	,
      29	->	28	,
      30	->	29	,
      31	->	30	,
      32	->	31	,
      33	->	32	,
      34	->	33	,
      35	->	34	,
      36	->	35	,
      37	->	36	,
      38	->	37	,
      39	->	38	,
      40	->	39	,
      41	->	40	,
      42	->	41	,
      43	->	42	,
      44	->	43	,
      45	->	44	,
      46	->	45
    )
    map
  }
  def get2ndChannelMap(int: Int): Map[Int,Int] = {
    val map: Map[Int,Int] = Map(
      601	->	0	,
      602	->	1	,
      603	->	2	,
      604	->	3	,
      605	->	4	,
      401	->	5	,
      402	->	6	,
      403	->	7	,
      404	->	8	,
      405	->	9	,
      406	->	10	,
      407	->	11	,
      408	->	12	,
      409	->	13	,
      410	->	14	,
      411	->	15	,
      412	->	16	,
      1501	->	17	,
      1502	->	18	,
      1503	->	19	,
      1504	->	20	,
      1505	->	21	,
      1506	->	22	,
      4401	->	23	,
      4402	->	24	,
      4403	->	25	,
      4404	->	26	,
      4405	->	27	,
      4406	->	28	,
      3301	->	29	,
      3302	->	30	,
      3303	->	31	,
      3304	->	32	,
      3305	->	33	,
      3306	->	34	,
      3307	->	35	,
      3308	->	36	,
      3309	->	37	,
      3310	->	38	,
      3311	->	39	,
      3312	->	40	,
      3313	->	41	,
      3314	->	42	,
      3315	->	43	,
      3316	->	44	,
      3317	->	45	,
      3318	->	46	,
      3319	->	47	,
      1301	->	48	,
      1302	->	49	,
      1303	->	50	,
      1304	->	51	,
      1305	->	52	,
      1306	->	53	,
      1307	->	54	,
      1308	->	55	,
      1309	->	56	,
      1310	->	57	,
      1311	->	58	,
      1312	->	59	,
      1313	->	60	,
      1314	->	61	,
      1315	->	62	,
      801	->	63	,
      802	->	64	,
      803	->	65	,
      804	->	66	,
      805	->	67	,
      806	->	68	,
      807	->	69	,
      808	->	70	,
      809	->	71	,
      810	->	72	,
      811	->	73	,
      1601	->	74	,
      1602	->	75	,
      1603	->	76	,
      1604	->	77	,
      1605	->	78	,
      1606	->	79	,
      1607	->	80	,
      1608	->	81	,
      1609	->	82	,
      1610	->	83	,
      1611	->	84	,
      1612	->	85	,
      1613	->	86	,
      1614	->	87	,
      1615	->	88	,
      1616	->	89	,
      1617	->	90	,
      1618	->	91	,
      1619	->	92	,
      1620	->	93	,
      3001	->	94	,
      3002	->	95	,
      3003	->	96	,
      3004	->	97	,
      301	->	98	,
      302	->	99	,
      303	->	100	,
      304	->	101	,
      305	->	102	,
      306	->	103	,
      307	->	104	,
      308	->	105	,
      309	->	106	,
      310	->	107	,
      311	->	108	,
      312	->	109	,
      313	->	110	,
      314	->	111	,
      315	->	112	,
      316	->	113	,
      317	->	114	,
      901	->	115	,
      902	->	116	,
      903	->	117	,
      904	->	118	,
      905	->	119	,
      906	->	120	,
      907	->	121	,
      908	->	122	,
      909	->	123	,
      910	->	124	,
      911	->	125	,
      912	->	126	,
      913	->	127	,
      914	->	128	,
      915	->	129	,
      916	->	130	,
      917	->	131	,
      918	->	132	,
      919	->	133	,
      920	->	134	,
      921	->	135	,
      1701	->	136	,
      1702	->	137	,
      1703	->	138	,
      1704	->	139	,
      1705	->	140	,
      1706	->	141	,
      1801	->	142	,
      1802	->	143	,
      1001	->	144	,
      1002	->	145	,
      1003	->	146	,
      1004	->	147	,
      1005	->	148	,
      1006	->	149	,
      1007	->	150	,
      1008	->	151	,
      1009	->	152	,
      1010	->	153	,
      1011	->	154	,
      1012	->	155	,
      1013	->	156	,
      1014	->	157	,
      1015	->	158	,
      1016	->	159	,
      1017	->	160	,
      1018	->	161	,
      1019	->	162	,
      1020	->	163	,
      1021	->	164	,
      1022	->	165	,
      2001	->	166	,
      2002	->	167	,
      2003	->	168	,
      2004	->	169	,
      2005	->	170	,
      2006	->	171	,
      2007	->	172	,
      2008	->	173	,
      2009	->	174	,
      2010	->	175	,
      2011	->	176	,
      2012	->	177	,
      2013	->	178	,
      2014	->	179	,
      2015	->	180	,
      2016	->	181	,
      2017	->	182	,
      2018	->	183	,
      2019	->	184	,
      2901	->	185	,
      2902	->	186	,
      2903	->	187	,
      2904	->	188	,
      2905	->	189	,
      2906	->	190	,
      2907	->	191	,
      2908	->	192	,
      2909	->	193	,
      2910	->	194	,
      2911	->	195	,
      2912	->	196	,
      2913	->	197	,
      2914	->	198	,
      2915	->	199	,
      2916	->	200	,
      701	->	201	,
      702	->	202	,
      703	->	203	,
      704	->	204	,
      705	->	205	,
      706	->	206	,
      707	->	207	,
      708	->	208	,
      709	->	209	,
      3801	->	210	,
      3802	->	211	,
      3803	->	212	,
      3804	->	213	,
      3805	->	214	,
      3806	->	215	,
      4001	->	216	,
      4002	->	217	,
      4003	->	218	,
      4004	->	219	,
      4005	->	220	,
      4006	->	221	,
      4007	->	222	,
      4008	->	223	,
      4009	->	224	,
      4010	->	225	,
      4011	->	226	,
      4012	->	227	,
      4013	->	228	,
      4014	->	229	,
      4015	->	230	,
      1101	->	231	,
      1102	->	232	,
      1103	->	233	,
      1104	->	234	,
      1105	->	235	,
      1106	->	236	,
      1107	->	237	,
      1108	->	238	,
      1109	->	239	,
      1110	->	240	,
      1111	->	241	,
      1112	->	242	,
      1113	->	243	,
      1114	->	244	,
      2301	->	245	,
      4301	->	246	,
      4302	->	247	,
      4303	->	248	,
      1401	->	249	,
      1402	->	250	,
      1901	->	251	,
      1902	->	252	,
      4201	->	253	,
      4202	->	254	,
      4203	->	255	,
      1201	->	256	,
      1202	->	257	,
      1203	->	258	,
      1204	->	259	,
      1205	->	260	,
      1206	->	261	,
      1207	->	262	,
      1208	->	263	,
      1209	->	264	,
      101	->	265	,
      102	->	266	,
      103	->	267	,
      104	->	268	,
      105	->	269	,
      106	->	270	,
      107	->	271	,
      108	->	272	,
      109	->	273	,
      110	->	274	,
      111	->	275	,
      112	->	276	,
      113	->	277	,
      114	->	278	,
      115	->	279	,
      116	->	280	,
      117	->	281	,
      118	->	282	,
      119	->	283	,
      120	->	284	,
      121	->	285	,
      122	->	286	,
      123	->	287	,
      124	->	288	,
      125	->	289	,
      126	->	290	,
      127	->	291	,
      128	->	292	,
      201	->	293	,
      202	->	294	,
      203	->	295	,
      204	->	296	,
      205	->	297	,
      206	->	298	,
      207	->	299	,
      208	->	300	,
      209	->	301	,
      210	->	302	,
      211	->	303	,
      212	->	304	,
      213	->	305	,
      214	->	306	,
      215	->	307	,
      216	->	308	,
      217	->	309	,
      218	->	310	,
      219	->	311	,
      220	->	312	,
      221	->	313	,
      222	->	314	,
      2101	->	315	,
      2102	->	316	,
      2103	->	317	,
      2104	->	318	,
      2105	->	319	,
      2106	->	320	,
      2107	->	321	,
      2108	->	322	,
      4601	->	323	,
      4602	->	324	,
      501	->	325	,
      502	->	326	,
      503	->	327	,
      504	->	328	,
      505	->	329	,
      506	->	330	,
      507	->	331	,
      508	->	332	,
      509	->	333	,
      510	->	334	,
      511	->	335	,
      520	->	336	,
      521	->	337	,
      522	->	338	,
      523	->	339	,
      524	->	340	,
      525	->	341	,
      526	->	342	,
      527	->	343	,
      528	->	344	,
      529	->	345	,
      530	->	346	,
      531	->	347	,
      532	->	348	,
      533	->	349
    )
    map
  }
  def transFeatureOneHot(originalData: DataFrame, dateStr: String): DataFrame = {

    // "date_key", "hour", "gender", "age", "city_level",
    // "login_freq" no-use, "active_level", "title_length",
    // "cover_pic", "quality_score", "account_score", "attract_point", "read_time",
    // "read_complete", "title_emotion", "channel_click_rate",
    // "channel_like_rate", "channel_comment_rate", "channel_id", "algorithm_rate",
    // "algorithm_id", "label", "uin", "article_id", "is_holiday","title_ellipsis",
    // "di_score", "di_usermodel_score","di_hot","di_tag_score","network",
    // "di_hqtag_score","account_read_score","account_click_score","account_like_score",
    // "account_comment_score","account_health_score","account_res_score"

    val indexer_hour = new StringIndexer()
      .setInputCol("hour")
      .setOutputCol("hour_index")
      .fit(originalData)
    val indexed_hour = indexer_hour.transform(originalData)
    val encoder_hour = new OneHotEncoder()
      .setInputCol("hour_index")
      .setOutputCol("hour_vec")
    val encoded_hour = encoder_hour.transform(indexed_hour)
    encoded_hour.show(5)

    // not in use
    val indexer_gender = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("gender_index")
      .fit(encoded_hour)
    val indexed_gender = indexer_gender.transform(encoded_hour)
    val encoder_gender = new OneHotEncoder()
      .setInputCol("gender_index")
      .setOutputCol("gender_vec")
    val encoded_gender = encoder_gender.transform(indexed_gender)
    encoded_gender.show(5)

    val indexer_age = new StringIndexer()
      .setInputCol("age")
      .setOutputCol("age_index")
      .fit(encoded_hour)
    val indexed_age = indexer_age.transform(encoded_hour)
    val encoder_age = new OneHotEncoder()
      .setInputCol("age_index")
      .setOutputCol("age_vec")
    val encoded_age = encoder_age.transform(indexed_age)
    encoded_age.show(5)

    // not in use
    val indexer_city = new StringIndexer()
      .setInputCol("city_level")
      .setOutputCol("city_level_index")
      .fit(encoded_age)
    val indexed_city = indexer_city.transform(encoded_age)
    val encoder_city = new OneHotEncoder()
      .setInputCol("city_level_index")
      .setOutputCol("city_level_vec")
    val encoded_city = encoder_city.transform(indexed_city)
    encoded_city.show(5)

    val indexer_title_length = new StringIndexer()
      .setInputCol("title_length")
      .setOutputCol("title_length_index")
      .fit(encoded_city)
    val indexed_title_length = indexer_title_length.transform(encoded_city)
    val encoder_title_length = new OneHotEncoder()
      .setInputCol("title_length_index")
      .setOutputCol("title_length_vec")
    val encoded_title_length = encoder_title_length.transform(indexed_title_length)
    encoded_title_length.show(5)

    val indexer_cover_pic = new StringIndexer()
      .setInputCol("cover_pic")
      .setOutputCol("cover_pic_index")
      .fit(encoded_title_length)
    val indexed_cover_pic = indexer_cover_pic.transform(encoded_title_length)
    val encoder_cover_pic = new OneHotEncoder()
      .setInputCol("cover_pic_index")
      .setOutputCol("cover_pic_vec")
    val encoded_cover_pic = encoder_cover_pic.transform(indexed_cover_pic)
    encoded_cover_pic.show(5)

    // not in use
    val indexer_title_emotion = new StringIndexer()
      .setInputCol("title_emotion")
      .setOutputCol("title_emotion_index")
      .fit(encoded_cover_pic)
    val indexed_title_emotion = indexer_title_emotion.transform(encoded_cover_pic)
    val encoder_title_emotion = new OneHotEncoder()
      .setInputCol("title_emotion_index")
      .setOutputCol("title_emotion_vec")
    val encoded_title_emotion = encoder_title_emotion.transform(indexed_title_emotion)
    encoded_title_emotion.show(5)

    val indexer_channel_id = new StringIndexer()
      .setInputCol("channel_id")
      .setOutputCol("channel_id_index")
      .fit(encoded_cover_pic)
    val indexed_channel_id = indexer_channel_id.transform(encoded_cover_pic)
    val encoder_channel_id = new OneHotEncoder()
      .setInputCol("channel_id_index")
      .setOutputCol("channel_id_vec")
    val encoded_channel_id = encoder_channel_id.transform(indexed_channel_id)
    encoded_channel_id.show(5)

    val indexer_algorithm_id = new StringIndexer()
      .setInputCol("algorithm_id")
      .setOutputCol("algorithm_id_index")
      .fit(encoded_channel_id)
    val indexed_algorithm_id = indexer_algorithm_id.transform(encoded_channel_id)
    val encoder_algorithm_id = new OneHotEncoder()
      .setInputCol("algorithm_id_index")
      .setOutputCol("algorithm_id_vec")
    val encoded_algorithm_id = encoder_algorithm_id.transform(indexed_algorithm_id)
    encoded_algorithm_id.show(5)

    // not in use
    val indexer_is_holiday = new StringIndexer()
      .setInputCol("is_holiday")
      .setOutputCol("is_holiday_index")
      .fit(encoded_algorithm_id)
    val indexed_is_holiday = indexer_is_holiday.transform(encoded_algorithm_id)
    val encoder_is_holiday = new OneHotEncoder()
      .setInputCol("is_holiday_index")
      .setOutputCol("is_holiday_vec")
    val encoded_is_holiday = encoder_is_holiday.transform(indexed_is_holiday)
    encoded_is_holiday.show(5)

    // not in use
    // the day of week, sparse to 7 dimensions
    val df = new SimpleDateFormat("yyyyMMdd")
    val date = df.parse(dateStr)
    val calendar: Calendar = Calendar.getInstance
    calendar.setTime(date)
    val dayIndex = calendar.get(Calendar.DAY_OF_WEEK)
    val encoded_temp = encoded_algorithm_id.withColumn("dayIndex", lit(dayIndex))
    val dayVector: (Long => Vector) = (arg: Long) => {
      try {
        Vectors.sparse(7, Array(arg.toInt - 1), Array(1))
      }
      catch {
        case ex: Exception => Vectors.sparse(7, Array(), Array())
      }
    }
    val dayFunc = udf(dayVector)
    val encoded_day_index = encoded_temp.withColumn("day_index_vec", dayFunc(col("dayIndex")))
    encoded_day_index.show(5)

    val indexer_title_ellipsis = new StringIndexer()
      .setInputCol("title_ellipsis")
      .setOutputCol("title_ellipsis_index")
      .fit(encoded_algorithm_id)
    val indexed_title_ellipsis = indexer_title_ellipsis.transform(encoded_algorithm_id)
    val encoder_title_ellipsis = new OneHotEncoder()
      .setInputCol("title_ellipsis_index")
      .setOutputCol("title_ellipsis_vec")
    val encoded_title_ellipsis = encoder_title_ellipsis.transform(indexed_title_ellipsis)
    encoded_title_ellipsis.show(5)

    // not in use
    val indexer_di_hot = new StringIndexer()
      .setInputCol("di_hot")
      .setOutputCol("di_hot_index")
      .fit(encoded_title_ellipsis)
    val indexed_di_hot = indexer_di_hot.transform(encoded_title_ellipsis)
    val encoder_di_hot = new OneHotEncoder()
      .setInputCol("di_hot_index")
      .setOutputCol("di_hot_vec")
    val encoded_di_hot = encoder_di_hot.transform(indexed_di_hot)
    encoded_di_hot.show(5)

    //not in use
    val indexer_network = new StringIndexer()
      .setInputCol("network")
      .setOutputCol("network_index")
      .fit(encoded_title_ellipsis)
    val indexed_network = indexer_network.transform(encoded_title_ellipsis)
    val encoder_network = new OneHotEncoder()
      .setInputCol("network_index")
      .setOutputCol("network_vec")
    val encoded_network = encoder_network.transform(indexed_network)
    encoded_network.show(5)

    //    "hour_vec", "gender_vec", "age_vec",
    //    "city_level_vec", "title_length_vec", "cover_pic_vec",
    //    "title_emotion_vec", "channel_id_vec", "algorithm_id_vec",
    //    "is_holiday_vec", "title_ellipsis_vec", "di_hot_vec",
    //    "network_vec", "active_level", "quality_score", "account_score",
    //    "attract_point", "read_time", "read_complete", "channel_click_rate",
    //    "channel_like_rate", "channel_comment_rate", "algorithm_rate",
    //    "di_score", "di_usermodel_score", "di_tag_score", "network", "di_hqtag_score"
    //    "account_read_score","account_click_score","account_like_score",
    //    "account_comment_score","account_health_score","account_res_score"
    val assemblerOutput = new VectorAssembler()
      .setInputCols(Array("hour_vec","age_vec",
        "title_length_vec", "cover_pic_vec",
        "channel_id_vec", "algorithm_id_vec",
        "day_index_vec", "title_ellipsis_vec",
        "avtive_level", "quality_score", "account_score",
        "read_time", "read_complete", "channel_click_rate",
        "channel_like_rate", "channel_comment_rate", "algorithm_rate",
        "di_score", "di_usermodel_score", "di_tag_score", "di_hqtag_score",
        "account_read_score","account_click_score","account_like_score",
        "account_comment_score","account_health_score","account_res_score"))
      .setOutputCol("features")
      .transform(encoded_title_ellipsis).select("article_id","uin","label","features")//.cache()

    assemblerOutput.show(5)
    assemblerOutput

  }
}
