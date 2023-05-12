package org.niit.service

import org.apache.spark.sql.Dataset
import org.niit.bean.OrderWithRecommendations
import org.niit.dao.BatchDataDao
import org.niit.util.SparkUtil

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:19
 * @desc:
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
    hotrestaurantCountTop50(allInfoDS)
    //需求二：
    hotrestaurantRecommendTop20(allInfoDS)
  }

  //需求一：找到Top50热点订单对应的餐厅，然后统计这些餐厅中，分别包含热点订单的数量 DSL语法实现
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
    val res = joinDF.groupBy("restaurant_id")
      .agg(count("*") as "hotCount")
      .orderBy('hotCount.desc)
    res.show()
  }

  //需求二： 各个科目推荐提分析
  /*
   找到前20热点题对应的推荐题目，然后找到推荐题目对应的科目，并统计每个科目分别包含推荐题目数量
   科目  推荐题数量
   数学   30
   语文   15
   英语   10
   */
  def hotrestaurantRecommendTop20(allInfoDS: Dataset[OrderWithRecommendations]): Unit = {
    //3.1统计前20道热点题，根据数量进行降序
    val hotTop20 = allInfoDS.groupBy('order_id)
      .agg(count("*") as "hot")
      .orderBy('hot.desc)
      .limit(20)
    //3.2将结果和allInfoDS进行关联，得到热点题的推荐列表
    val ridsDF = hotTop20.join(allInfoDS, "order_id").select("recommendations")
    //3.3将获得到的推荐列表（ridsDF），转换成数据
    /*
    推荐列表：“题目ID_1021,题目ID_1812,题目ID_1555,题目ID_1233,题目ID_171,题目ID_2124,题目ID_1105,题目ID_589,题目ID_1427,题目ID_1738”
     Split(",")  切割之后去重
     */
    val ridsDS = ridsDF.select(explode(split('recommendations, ",")) as "order_id").dropDuplicates("order_id")
    //3.4将 ridsDS 和 allInfoDS进行关联，得到每个推荐题目所属的科目
    val ridAndSid = ridsDS.join(allInfoDS.dropDuplicates("order_id"), "order_id")
    //3.5统计各个科目包含的推荐题目数量，并降序排序
    val res = ridAndSid.groupBy('restaurant_id)
      .agg(count("*") as "rcount")
      .orderBy('rcount.desc)
    //3.6输出
    res.show()
  }

}
