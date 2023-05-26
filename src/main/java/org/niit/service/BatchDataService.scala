package org.niit.service

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{Dataset, SaveMode}
import org.niit.bean.{OrderWithRecommendations, Orders}
import org.niit.dao.{BatchDataDao, BatchDataDao1}
import org.niit.util.{JDBCUtil, SparkUtil}

import java.util.Properties

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:19
 * @desc:离线服务层
 */
//离线批处理服务
class BatchDataService {

  val spark = SparkUtil.takeSpark()
  // 使用conn进行数据库操作
//  val conn = JDBCUtil.getConnection

  val url = "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8"
  val dbProperties = new Properties()
  dbProperties.setProperty("user", "root")
  dbProperties.setProperty("password", "Niit@123")
  dbProperties.setProperty("driver", "com.mysql.jdbc.Driver")

  import org.apache.spark.sql.functions._
  import spark.implicits._


  def recommendService(): Unit = {
    //离线推荐，对历史数据进行分析历史数据一般会存在数据库中（MySQL/HBase）
    //1.连接数据库
    val path = "output/als_movie_model_test/1684896788168"
    val takeawayDao1 = new BatchDataDao1
    val allInfoDS = takeawayDao1.getTakeawayData()
    allInfoDS.show()
    //离线推荐一：根据用户推荐10个餐厅
    recommendForAllUsers(allInfoDS, path)
    //离线推荐二：根据餐厅推荐3个用户
    recommendForAllItems(allInfoDS,path)
  }
  def dataAnalysis(): Unit = {
    //离线分析，对历史数据进行分析历史数据一般会存在数据库中（MySQL/HBase）
    //1.连接数据库
    //应该放在dao层
    val takeawayDao = new BatchDataDao
    val allInfoDS = takeawayDao.getTakeawayData()
    //需求一：
    hotCityCountTop10(allInfoDS)
    //需求二：
    hotfoodcategoryRecommendTop10(allInfoDS)
    //需求三：
    hotfoodcategoryTop3(allInfoDS)
    //需求四：
    hotrestaurantTop10(allInfoDS)
  }

  def hotCityCountTop10(allInfoDS: Dataset[OrderWithRecommendations]): Unit = {
    //2.1统计前50道热点题 ----->>在数据库中，即使相同的题目，也是分布在不同行中的
    //张三  题目1  数学
    //李四  题目1  数学  =>题目1  2
    val hotTop50 = allInfoDS.groupBy("order_id")
      .agg(count("*") as "hotCount")
      .orderBy('hotCount.desc)
    //      .limit(10)
    //2.2将hotTop50和allInfoDS进行关联，得到热点题对应的题目 dropDuplicates:去重
    val joinDF = hotTop50.join(allInfoDS.dropDuplicates("order_id"), "order_id")
    //2.3按学科分组聚合各个学科包含热点题的数量
    val res = joinDF.groupBy("city_id")
      .agg(count("*") as "hotCount")
      .orderBy('hotCount.desc)
    println("--------统计推荐所属的热门城市Top10--------")
    res.toDF().write.mode(SaveMode.Overwrite)
      .jdbc(url, "Batch_Hot_City_Top10", dbProperties)
    res.show()
  }

  //需求二： 每个热门食品类别推荐前十个餐厅
  /*
   找到前20热点订单对应的推荐餐厅，然后找到推荐餐厅对应的用户，并统计每个用户分别包含推荐餐厅数量

   */
  def hotfoodcategoryRecommendTop10(allInfoDS: Dataset[OrderWithRecommendations]): Unit = {

    //3.1统计个热门菜品类别，根据数量进行降序
    val hotTop20 = allInfoDS
      .sort('score
      ).groupBy('food_category_id)
      .agg(count("*") as "hot")
      .orderBy('hot.desc)
    //      .limit(20)


    //3.2将结果和allInfoDS进行关联，得到热点餐厅的推荐列表
    val ridsDF = hotTop20.join(allInfoDS, "food_category_id").select("food_category_id",
      "recommendations")
    //    ridsDF.show(false)
    //3.3将获得到的推荐列表（ridsDF），转换成数据

    /*
    推荐列表：“餐厅ID_2908,餐厅ID_3162,餐厅ID_2372,餐厅ID_644,餐厅ID_206,餐厅ID_3185,餐厅ID_2895,餐厅ID_402,餐厅ID_1338,餐厅ID_3124,餐厅ID_2546,餐厅ID_999,餐厅ID_1229,餐厅ID_2862,餐厅ID_616,餐厅ID_2350,餐厅ID_1843,餐厅ID_783,餐厅ID_1027,餐厅ID_1274”
     Split(",")  切割之后去重
     */
    //    val ridsDS = ridsDF.select(explode(split('recommendations, ",")) as "restaurant_id").dropDuplicates("restaurant_id")
    val ridsDS = ridsDF.select(explode(split('recommendations, ",")) as "restaurant_id")
    //    ridsDS.show(false)
    //3.4将 ridsDS 和 allInfoDS进行关联，得到每个推荐餐厅所属的菜品
    val ridAndSid = ridsDS.join(allInfoDS.dropDuplicates("restaurant_id"), "restaurant_id")
    ridAndSid.write.mode(SaveMode.Overwrite)
      .jdbc(url, "Batch_Hot_Food_Top10", dbProperties)

    //3.5统计各个科目包含的推荐题目数量，并降序排序
    val res = ridAndSid.groupBy('restaurant_id)
      .agg(count("recommendations") as "rcount")
      .orderBy('rcount.desc)
    //3.6输出
    println("---------每个热门食品类别推荐前十个餐厅------------")
    res.toDF().write.mode(SaveMode.Overwrite)
      .jdbc(url,
        "Batch_Hot_Canteen_Top10", dbProperties)
    res.show(10)
  }

  //需求三：统计热门菜品Top10
  def hotfoodcategoryTop3(allInfoDS: Dataset[OrderWithRecommendations]): Unit = {
    val foodhotTop10 = allInfoDS.groupBy('food_category_id)
      .agg(count("*") as 'hot)
      .orderBy('hot.desc)
    println("---------统计热门菜品Top10------------")
    foodhotTop10.toDF().write.mode(SaveMode.Overwrite)
      .jdbc(url,
        "Batch_Hot_Food_Top3", dbProperties)
    foodhotTop10.show(3)
  }

  //需求四：热门餐厅
  def hotrestaurantTop10(allInfoDS: Dataset[OrderWithRecommendations]): Unit = {
    val hotrestaurantTop10 = allInfoDS.groupBy('restaurant_id)
      .agg(count("*") as 'hot)
      .orderBy('hot.desc)
    println("---------统计热门餐厅Top10------------")
    hotrestaurantTop10.toDF().write.mode(SaveMode.Overwrite)
      .jdbc(url, "Batch_Hot_Canteen_Top10", dbProperties)
    hotrestaurantTop10.show(10)
  }


  //离线推荐一：根据用户推荐10个餐厅
  def recommendForAllUsers(allInfoDS: Dataset[Orders], path: String) = {
    val model = ALSModel.load(path)
//    import org.apache.spark.sql.functions.udf
    // 定义UDF，生成整数ID
    val userid2Int = udf((user_id: String) => {
      user_id.split("_")(1).toInt //根据 _ 分割数组，分割后取第二个元素为id，并调用toInt将其转换为整数类型
    })
    val restaurantId2Int = udf((restaurant_id: String) => {
      restaurant_id.split("_")(1).toInt
    })

    val ordersDF = allInfoDS.toDF()
    val userIdDF = ordersDF.select(userid2Int('user_id) as "user_id") // 生成用户ID整数列

    //5.使用模型给用户推荐餐厅  推荐10个高质量餐厅
    //调用协同过滤模型 model 的 recommendForUserSubset() 方法。
    // 该方法需要传入两个参数：一个包含用户ID的DataFrame，和一个整数类型的参数 numItems，表示要返回的每个用户的推荐项目数量。
    val userRecs = model.recommendForUserSubset(userIdDF, 10)

    //false 显示的时候。将省略的信息也显示出来
    userRecs.show(false) //执行 show() 方法,参数设置为 false，以便查看所有列的完整信息。


    //6.处理推荐结果： 取出学生id和餐厅id，拼接成字符串：id1,id2
    val userRecsDF = userRecs.as[(Int, Array[(Int, Float)])].map(t => {
      val userId: String = "用户ID_" + t._1
      val restaurantId = t._2.map("餐厅ID_" + _._1).mkString(",")
      (userId, restaurantId)
    }).toDF("user_id", "recommendations")

    val allInfoDF = ordersDF.join(userRecsDF,"user_id").select("user_id","recommendations")
    // 将推荐结果存储到 MySQL 中
    val rectable = "recommendations_for_user"
    allInfoDF.write.mode(SaveMode.Append)
     .jdbc(url,rectable,dbProperties)


  }

  //离线推荐二：根据餐厅推荐3个用户
  def recommendForAllItems(allInfoDS: Dataset[Orders],path:String): Unit = {
    val model = ALSModel.load(path)

    // 定义UDF，生成整数ID
    val restaurantId2Int = udf((restaurant_id: String) => {
      restaurant_id.split("_")(1).toInt
    })

    val ordersDF = allInfoDS.toDF()
    val restaurantId = ordersDF.select(restaurantId2Int('restaurant_id) as "restaurant_id")

    //5.使用模型给用户推荐餐厅  推荐10个高质量客户
    //调用协同过滤模型 model 的 recommendForAllItems() 方法。
    val restRecs = model.recommendForAllItems(10)
    restRecs.show(false)


    //6.处理推荐结果： 取出学生id和餐厅id，拼接成字符串：id1,id2
    val userRecsDF = restRecs.as[(Int, Array[(Int, Float)])].map(t => {
      val restaurantId: String = "餐厅ID_" + t._1
      val userId = t._2.map("客户ID_" + _._1).mkString(",")
      (restaurantId, userId)
    }).toDF("restaurant_id", "recommendations")

    val allInfoDF1 = ordersDF.join(userRecsDF, "restaurant_id").select("restaurant_id", "recommendations")
    // 将推荐结果存储到 MySQL 中
    val rectable = "recommendations_for_restaurant"
    allInfoDF1.write.mode(SaveMode.Append)
      .jdbc(url,rectable,dbProperties)

  }
//  conn.close()
}
