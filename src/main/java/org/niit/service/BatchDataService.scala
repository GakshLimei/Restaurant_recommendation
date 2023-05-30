package org.niit.service

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SaveMode}
import org.niit.bean.Orders2MySQL
import org.niit.dao.BatchDataDao
import org.niit.util.SparkUtil

import java.util.Properties

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:19
 * @desc:离线服务层
 */
//离线批处理服务
class BatchDataService extends Serializable {

  val spark = SparkUtil.takeSpark()
  val url = "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8"
  val dbProperties = new Properties()
  dbProperties.setProperty("user", "root")
  dbProperties.setProperty("password", "Niit@123")
  dbProperties.setProperty("driver", "com.mysql.jdbc.Driver")

  import org.apache.spark.sql.functions._
  import spark.implicits._


  def dataAnalysis(): Unit = {
    //离线分析，对历史数据进行分析历史数据一般会存在数据库中（MySQL/HBase）
    //1.连接数据库
    //应该放在dao层
    val takeawayDao = new BatchDataDao
    val allInfoDS = takeawayDao.getTakeawayData()
    //需求一：离线统计离线统计热门菜品Top5
        hotCityCountTop5(allInfoDS)
    //需求二：离线统计高评分餐厅Top10
        highRatingRestaurantTop10(allInfoDS)
    //需求三：离线统计热门菜品Top3
        hotFoodCategoryTop3(allInfoDS)
    //需求四：离线统计热门餐厅Top10
        hotrestaurantTop10(allInfoDS)
  }


  def recommendService(): Unit = {
    //离线推荐，对历史数据进行分析历史数据一般会存在数据库中（MySQL/HBase）
    //模型路径
    val path = "output/batch_als_order_model/1685157225248"
    //1.连接数据库
    val takeawayDao = new BatchDataDao
    val allInfoDS = takeawayDao.getTakeawayData()
    allInfoDS.show()
    //离线推荐一：根据用户推荐10个餐厅
    recommendForAllUsers(allInfoDS, path)
    //离线推荐二：根据餐厅推荐3个用户
    recommendForAllItems(allInfoDS, path)
  }

  //离线推荐一：根据用户推荐10个餐厅
  def recommendForAllUsers(allInfoDS: Dataset[Orders2MySQL], path: String) = {
    val model = ALSModel.load(path)
    //    import org.apache.spark.sql.functions.udf
    // 定义UDF，生成整数ID


    val ordersDF = allInfoDS.toDF()
    val userIdDF = ordersDF.select("user_id")

    //5.使用模型给用户推荐餐厅  推荐10个高质量餐厅
    //调用协同过滤模型 model 的 recommendForUserSubset() 方法。
    // 该方法需要传入两个参数：一个包含用户ID的DataFrame，和一个整数类型的参数 numItems，表示要返回的每个用户的推荐项目数量。
    val userRecs = model.recommendForUserSubset(userIdDF, 10)

    //false 显示的时候。将省略的信息也显示出来
    println("---------离线推荐根据用户推荐10个餐厅---------")
    userRecs.show(false) //执行 show() 方法,参数设置为 false，以便查看所有列的完整信息。


    //6.处理推荐结果： 取出学生id和餐厅id，拼接成字符串：id1,id2
    val userRecsDF = userRecs.as[(Int, Array[(Int, Float)])].map(t => {
      val userId: Int = t._1
      val restaurantId = t._2.map("餐厅ID_" + _._1).mkString(",")
      (userId, restaurantId)
    }).toDF("user_id", "recommendations")

    val allInfoDF = ordersDF.join(userRecsDF, "user_id").select("user_id", "recommendations")
    // 将推荐结果存储到 MySQL 中
    val rectable = "Batch_recommendations_for_user"
    allInfoDF.write.mode(SaveMode.Overwrite)
      .jdbc(url, rectable, dbProperties)


  }

  //离线推荐二：根据餐厅推荐3个用户
  def recommendForAllItems(allInfoDS: Dataset[Orders2MySQL], path: String): Unit = {
    val model = ALSModel.load(path)

    val ordersDF = allInfoDS.toDF()

    //5.使用模型给用户推荐餐厅  推荐10个高质量客户
    //调用协同过滤模型 model 的 recommendForAllItems() 方法。
    val restRecs = model.recommendForAllItems(10)
    println("---------离线推荐根据餐厅推荐3个用户---------")
    restRecs.show(false)


    //6.处理推荐结果： 取出学生id和餐厅id，拼接成字符串：id1,id2
    val userRecsDF = restRecs.as[(Int, Array[(Int, Float)])].map(t => {
      val restaurantId: Int = t._1
      val userId = t._2.map("客户ID_" + _._1).mkString(",")
      (restaurantId, userId)
    }).toDF("restaurant_id", "recommendations")

    val allInfoDF1 = ordersDF.join(userRecsDF, "restaurant_id").select("restaurant_id", "recommendations")
    // 将推荐结果存储到 MySQL 中
    val rectable = "Batch_recommendations_for_restaurant"
    allInfoDF1.write.mode(SaveMode.Overwrite)
      .jdbc(url, rectable, dbProperties)

  }

  //需求一：离线统计热门城市Top5
  def hotCityCountTop5(allInfoDS: Dataset[Orders2MySQL]): Unit = {
    // 根据城市分组并统计数量
    val cityCountDS = allInfoDS.groupBy($"city_name").count()

    // 按照数量倒序排序并取前5个
    val top5DS = cityCountDS.orderBy($"count".desc).limit(5)
    top5DS.write.mode(SaveMode.Overwrite)
      .jdbc(url, "Batch_Hot_City_Top10", dbProperties)

    println("---------离线统计热门城市Top3---------")
    // 显示结果
    top5DS.show()

  }

  //需求二： 离线统计高评分餐厅Top10
  def highRatingRestaurantTop10(allInfoDS: Dataset[Orders2MySQL]) = {
    // 先按照餐厅ID和评分进行分组，统计每个餐厅的平均评分
    val restaurantAvgScoreDF = allInfoDS.groupBy($"restaurant_id")
      .agg(avg($"score").as("avg_score"))


    // 再按照餐厅ID进行分组，统计每个餐厅的平均评分排名
    val restaurantRankDF = restaurantAvgScoreDF
      .withColumn("rank", dense_rank().over(Window.orderBy(desc("avg_score"))))
      .filter($"rank" <= 10)


    // 通过关联获取餐厅信息
    println("---------离线统计高评分餐厅Top10---------")
    val rids = allInfoDS.join(restaurantRankDF, Seq("restaurant_id"))
    rids.select($"restaurant_id", $"avg_score", $"rank")
      .distinct()
      .orderBy("rank")
      .limit(10)
      .show()

    rids.write.mode(SaveMode.Overwrite)
      .jdbc(url, "Batch_Rating_Restaurant_Top10", dbProperties)
  }

  //需求三：离线统计热门菜品Top3
  def hotFoodCategoryTop3(allInfoDS: Dataset[Orders2MySQL]): Unit = {
    val foodhotTop3 = allInfoDS.groupBy('food_category)
      .agg(count("*") as 'hot)
      .orderBy('hot.desc)
      .limit(3)

    println("---------离线统计热门菜品Top3---------")
    foodhotTop3.toDF().write.mode(SaveMode.Overwrite)
      .jdbc(url,
        "Batch_Hot_Food_Top3", dbProperties)
    foodhotTop3.show()
  }

  //需求四：离线统计热门餐厅Top10
  def hotrestaurantTop10(allInfoDS: Dataset[Orders2MySQL]): Unit = {
    val hotrestaurantTop10 = allInfoDS.groupBy(Symbol("restaurant_id"))
      .agg(count("*") as 'hot)
      .orderBy('hot.desc)
      .limit(3)
    println("---------离线统计热门餐厅Top10---------")
    hotrestaurantTop10.toDF().write.mode(SaveMode.Overwrite)
      .jdbc(url, "Batch_Hot_Canteen_Top10", dbProperties)
    hotrestaurantTop10.show(10)
  }

}
