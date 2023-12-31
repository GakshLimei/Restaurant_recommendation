package org.niit.service


import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders
import org.niit.util.SparkUtil

import java.util.Properties

/**
 * @作者 YanTianCheng
 * @时间 2023/5/3 20:55
 * @文件: RecommendService
 * @项目 org.niit.service
 *       推荐系统
 */
class RecommendService {
  val spark = SparkUtil.takeSpark()

  import spark.implicits._

  def dataAnalysis(orders: DStream[Orders]): Unit = {
    orders.foreachRDD(rdd => {
      //1.获取训练好的模型路径
      //如果是存在HBase中
      //      HBaseUtil.setHTable("bigdata:takeaway")
      //      val value = HBaseUtil.getData(new Get(Bytes.toBytes("als_model-recommended_orders_id")))
      //      val path = value(0)
      val path = "output/als_model/1685549315179"

      //2.加载模型
      val model = ALSModel.load(path)

      //3.由于在ALS推荐算法只能存储纯数字东西（用户ID—32 =32）所以在后面使用模型的时候也需要将要读取的数据截取
      //      val id2Int = udf((user_id:String) =>{
      //        user_id.split("_")(1).toInt    //根据 _ 分割数组，分割后取第二个元素为id，并调用toInt将其转换为整数类型
      //      })
      //      val restaurantInt = udf((restaurant_id:String) =>{
      //        restaurant_id.split("_")(1).toInt
      //      })

      //4.由于SparkMlib的模型只能加载sparkSQL 所以需要rdd--》dataFrame
      val ordersDF = rdd.toDF() //将RDD转换为DataFrame，将其命名为 ordersDF
      //调用 select 方法并传入 id2Int('user_id) 表达式，以将 user_id 转换为整数类型,使用 as 关键字给新生成的列起个别名 user_id
      val userIdDF = ordersDF.select("user_id")
      val restaurantId = ordersDF.select("restaurant_id")

      //5.使用模型给用户推荐餐厅  推荐3个高质量餐厅
      //调用协同过滤模型 model 的 recommendForUserSubset() 方法。
      // 该方法需要传入两个参数：一个包含用户ID的DataFrame，和一个整数类型的参数 numItems，表示要返回的每个用户的推荐项目数量。
      val recommendDF = model.recommendForUserSubset(userIdDF, 3)
      val recommendDF2 = model.recommendForItemSubset(restaurantId, 3)
      //      //false 显示的时候。将省略的信息也显示出来
      recommendDF.show(false) //执行 show() 方法,参数设置为 false，以便查看所有列的完整信息。
      recommendDF2.show(false)
      //
      //      //6.处理推荐结果： 取出学生id和餐厅id，拼接成字符串：id1,id2
      val recommendedDF = recommendDF.as[(Int, Array[(Int, Float)])].map(t => {
        val userId: Int = t._1
        val restaurantId = t._2.map("餐厅ID_" + _._1).mkString(",")
        (userId, restaurantId)
      }).toDF("user_id", "recommendations")

      val recommendedDF2 = recommendDF2.as[(Int, Array[(Int, Float)])].map(t => {
        val restaurantId: Int = t._1
        val userId = t._2.map("用户ID_" + _._1).mkString(",")
        (restaurantId, userId)
      }).toDF("restaurant_id", "recommendations")
      //      //将 DataFrame 转换为 RDD
      //      //将 RDD 转换回 DataFrame，并将 user_id 和 recommendations 列分别作为 DataFrame 的 user_id 和 recommendations 列
      //      //将 RDD 转换为 DataFrame，并将 user_id 和 recommendations 列分别作为 DataFrame 的 user_id 和 recommendations 列
      //
      //      //7.将kafka中的orders 数据和recommendDF进行合并
      val allInfoDF = ordersDF.join(recommendedDF, "user_id").select("user_id", "recommendations")
      //
      val allInfoDF2 = ordersDF.join(recommendedDF2, "restaurant_id").select("restaurant_id", "recommendations")


      // 3. 设置数据库连接属性
      val dbProperties = new Properties()
      dbProperties.put("user", "root")
      dbProperties.put("password", "Niit@123")
      dbProperties.put("driver", "com.mysql.jdbc.Driver")

      allInfoDF.write.mode(SaveMode.Append)
        .jdbc("jdbc:mysql://Node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8",
          "Recommend_restaurant", dbProperties)

      allInfoDF2.write.mode(SaveMode.Append)
        .jdbc("jdbc:mysql://Node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8",
          "Recommend_user", dbProperties)

    })

  }
}
