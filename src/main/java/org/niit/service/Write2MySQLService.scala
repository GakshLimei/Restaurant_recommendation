package org.niit.service

import org.apache.spark.sql.SaveMode
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
      val cityId = data.city_id
      val appID = data.app_id
      val foodID = data.food_category_id
      val newUserId = data.user_id.replace("用户ID_", "")
      val newRestaurantId = data.restaurant_id.replace("餐厅ID_", "")
      val newOrderId = data.order_id.replace("订单ID_", "")

      val newFoodId = foodID match {
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
      val newAppId = appID match {
        case "平台ID_1" => "美团"
        case "平台ID_2" => "饿了么"
      }
      val newCityId = cityId match {
        case "城市ID_1" => "北京"
        case "城市ID_2" => "上海"
        case "城市ID_3" => "广州"
        case "城市ID_4" => "深圳"
        case "城市ID_5" => "南宁"
        case "城市ID_6" => "柳州"
        case "城市ID_7" => "桂林"
        case "城市ID_8" => "北海"
        case "城市ID_9" => "天津"
        case "城市ID_10" => "济南"
      }
      data.copy(user_id = newUserId,app_id = newAppId,city_id = newCityId,restaurant_id = newRestaurantId , food_category_id = newFoodId,order_id = newOrderId)

    }).foreachRDD(rdd => {


      val ordersDF = rdd.toDF()

      ordersDF.write
        .format("jdbc")
        .option("url", "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "orders")
        .option("user", "root")
        .option("password", "Niit@123")
        .mode(SaveMode.Append)
        .save()
      println("Data successfully written to MySQL database.")
    })
  }

}
