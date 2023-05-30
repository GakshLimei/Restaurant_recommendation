package org.niit.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders
import org.niit.util.SparkUtil

/**
 *
 * @author: Gary Chen
 * @Created: 2023/5/25 11:55
 * @desc: Write2MySQL服务层
 *
 */
class Write2MySQLService() {
  val spark = SparkUtil.takeSpark()

  import spark.implicits._

  def writetomysql(orders: DStream[Orders]): Unit = {
    orders.map(data => {
      val newUserId = data.user_id.replace("用户ID_", "")
      val newRestaurantId = data.restaurant_id.replace("餐厅ID_", "")
      val newOrderId = data.order_id.replace("订单ID_", "")
      data.copy(user_id = newUserId, restaurant_id = newRestaurantId, order_id = newOrderId)
    }).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val ordersDF = rdd.toDF("user_id", "app_id", "city_id", "restaurant_id", "food_category_id", "order_id", "score", "order_time", "ts")
//        ordersDF.show


        val foodid_udf = udf((rawFoodID: String) => {
          rawFoodID match {
            case "菜品ID_category_1" => "热菜"
            case "菜品ID_category_2" => "凉菜"
            case "菜品ID_category_3" => "汤类"
            case "菜品ID_category_4" => "火锅"
            case "菜品ID_category_5" => "快餐"
            case "菜品ID_category_6" => "烧烤"
            case "菜品ID_category_7" => "粉面"
            case "菜品ID_category_8" => "披萨意面"
            case "菜品ID_category_9" => "包子馒头"
            case "菜品ID_category_10" => "特色菜"
          }
        })
        val appid_udf = udf((rawAppID: String) => {
          rawAppID match {
            case "平台ID_1" => "美团"
            case "平台ID_2" => "饿了么"
          }
        })
        val cityid_udf = udf((rawCityID: String) => {
          rawCityID match {
            case "城市ID_1" => "崇左"
            case "城市ID_2" => "来宾"
            case "城市ID_3" => "钦州"
            case "城市ID_4" => "防城港"
            case "城市ID_5" => "南宁"
            case "城市ID_6" => "柳州"
            case "城市ID_7" => "桂林"
            case "城市ID_8" => "北海"
            case "城市ID_9" => "玉林"
            case "城市ID_10" => "百色"
          }
        })


        //        val ordersDF = rdd.toDF("user_id", "app_id", "city_id", "restaurant_id", "food_category_id", "order_id", "score", "order_time", "ts")

        val transformDF = ordersDF.withColumn("food_category", foodid_udf($"food_category_id"))
          .withColumn("app_name", appid_udf($"app_id"))
          .withColumn("city_name", cityid_udf($"city_id"))
          .drop("city_id", "app_id", "food_category_id")
          .select("user_id", "app_name", "city_name", "restaurant_id", "food_category", "order_id", "score", "order_time", "ts")


        transformDF.show
        transformDF.write
          .format("jdbc")
          .option("url", "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", "history_orders")
          .option("user", "root")
          .option("password", "Niit@123")
          .option("batchsize", "1000") // 指定批量写入大小
          //          .option("createTableOptions", "PRIMARY KEY (order_id)")
          .option("createTablePrimaryKey", "order_id")
          //          .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='订单表' PRIMARY KEY (order_id)")
          .option("createTableColumnTypes", "user_id varchar(10), app_name varchar(10) , city_name varchar(10), restaurant_id varchar(10), food_category varchar(10) , order_id varchar(10)")
          .mode(SaveMode.Append)
          .save()
        println("Data successfully written to MySQL database.")
      }
    })
  }

}
