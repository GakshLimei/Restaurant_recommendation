package org.niit.service

import org.apache.spark.sql.Dataset
import org.niit.bean.OrderWithRecommendations
import org.niit.dao.BatchDataDao
import org.niit.util.SparkUtil

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:19
 * @desc:离线分析服务层
 */
//离线批处理服务
class BatchDataService {

  val spark = SparkUtil.takeSpark()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  def dataAnalysis(): Unit = {
    //离线分析，对历史数据进行分析历史数据一般会存在数据库中（MySQL/HBase）
    //1.连接数据库
    //应该放在dao层
    val takeawayDao = new BatchDataDao
    val allInfoDS = takeawayDao.getTakeawayData()
    //需求一：
//    hotrestaurantCountTop50(allInfoDS)
    //需求二：
    hotrestaurantRecommendTop20(allInfoDS)
  }


  def hotrestaurantCountTop50(allInfoDS: Dataset[OrderWithRecommendations]): Unit = {
    //2.1统计前50道热点题 ----->>在数据库中，即使相同的题目，也是分布在不同行中的
    //张三  题目1  数学
    //李四  题目1  数学  =>题目1  2
    val hotTop50 = allInfoDS.groupBy("order_id")
      .agg(count("*") as "hotCount")
      .orderBy('hotCount.desc)
      .limit(50)
    //2.2将hotTop50和allInfoDS进行关联，得到热点题对应的题目 dropDuplicates:去重
    val joinDF = hotTop50.join(allInfoDS.dropDuplicates("order_id"), "order_id")
    //2.3按学科分组聚合各个学科包含热点题的数量
    val res = joinDF.groupBy("city_id")
      .agg(count("*") as "hotCount")
      .orderBy('hotCount.desc)
    res.show()
  }

  //需求二： 各个科目推荐提分析
  /*
   找到前20热点订单对应的推荐餐厅，然后找到推荐餐厅对应的用户，并统计每个用户分别包含推荐餐厅数量
   科目  推荐题数量
   数学   30
   语文   15
   英语   10
   */
  def hotrestaurantRecommendTop20(allInfoDS: Dataset[OrderWithRecommendations]): Unit = {

    //3.1统计前20个热点餐厅，根据数量进行降序
    val hotTop20 = allInfoDS.groupBy('food_category_id)
      .agg(count("*") as "hot")
      .orderBy('hot.desc)
      .limit(20)

    //3.2将结果和allInfoDS进行关联，得到热点题的推荐列表
    val ridsDF = hotTop20.join(allInfoDS, "food_category_id").select("recommendations")
    ridsDF.show(false)
    //3.3将获得到的推荐列表（ridsDF），转换成数据
    /*
    推荐列表：“餐厅ID_2908,餐厅ID_3162,餐厅ID_2372,餐厅ID_644,餐厅ID_206,餐厅ID_3185,餐厅ID_2895,餐厅ID_402,餐厅ID_1338,餐厅ID_3124,餐厅ID_2546,餐厅ID_999,餐厅ID_1229,餐厅ID_2862,餐厅ID_616,餐厅ID_2350,餐厅ID_1843,餐厅ID_783,餐厅ID_1027,餐厅ID_1274”
     Split(",")  切割之后去重
     */
    val ridsDS = ridsDF.select(explode(split('recommendations, ",")) as "restaurant_id").dropDuplicates("restaurant_id")
//    val ridsDS = ridsDF.select(explode(split('recommendations, ",")) as "restaurant_id")
    ridsDS.show(false)
    //3.4将 ridsDS 和 allInfoDS进行关联，得到每个推荐题目所属的科目
    val ridAndSid = ridsDS.join(allInfoDS.dropDuplicates("restaurant_id"), "restaurant_id")
    ridAndSid.show(false)
    //3.5统计各个科目包含的推荐题目数量，并降序排序
    val res = ridAndSid.groupBy('food_category_id)
      .agg(count("*") as "rcount")
      .orderBy('rcount.desc)
    //3.6输出
    res.show()
  }

}
