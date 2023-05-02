package org.niit.mock

import com.google.gson.Gson
import org.niit.bean.Orders
import org.niit.mock.Simulator.genOrder

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * @author: Gary Chen
 * @Created: 2023/4/28 18:38
 * @desc: 模拟数据生成器
 */

/**
 * 在线教育学生学习数据模拟程序
 */
object Simulator {
  //模拟数据
  //用户ID
  val arr1 = ArrayBuffer[String]()
  for (i <- 1 to 50) {
    arr1 += "用户ID_" + i
  }
  //平台ID
  val arr2 = Array("平台ID_1", "平台ID_2")
  //城市ID
  val arr3 = Array("城市ID_1", "城市ID_2", "城市ID_3", "城市ID_4", "城市ID_5", "城市ID_6")
  //餐厅ID
  val arr4 = Array("餐厅ID_1_", "餐厅ID_2_", "餐厅ID_3_", "餐厅ID_4_")
  //菜品ID
  val arr5 = Array("菜品ID_category_1", "菜品ID_category_2", "菜品ID_category_3")

  //订单ID与平台、城市、餐厅、菜品的对应关系,
  val ordersMap = collection.mutable.HashMap[String, ArrayBuffer[String]]()

  var ordersID = 1
  for (appID <- arr2; cityID <- arr3; restaurantID <- arr4; food_categoryID <- arr5) {
    val key = new StringBuilder()
      .append(appID).append("^")
      .append(cityID).append("^")
      .append(restaurantID).append("^")
      .append(food_categoryID)

    val ordersArr = ArrayBuffer[String]()
    for (i <- 1 to 20) {
      ordersArr += "订单ID_" + ordersID
      ordersID += 1
    }
    ordersMap.put(key.toString(), ordersArr)
  }
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  def genOrder() = {
    //随机平台ID
    val appIDRandom = arr2(Random.nextInt(arr2.length))
    //随机城市ID
    val cityIDRandom = arr3(Random.nextInt(arr3.length))
    //随机餐厅ID
    val restaurantIDRandom = arr4(Random.nextInt(arr4.length))
    //随机菜品ID
    val food_categoryIDRandom = arr5(Random.nextInt(arr5.length))

    val key = new StringBuilder()
      .append(appIDRandom).append("^")
      .append(cityIDRandom).append("^")
      .append(restaurantIDRandom).append("^")
      .append(food_categoryIDRandom)

    //取出订单
    val ordersArr = ordersMap(key.toString())
    //随机订单ID
    val ordersRandom = ordersArr(Random.nextInt(ordersArr.length))
    //随机订单评分扣分
    val ordersScoreRandom = Random.nextInt(11) + 1 //Kotlin中的Random类的方法，表示生成一个0到10（不包含10）之间的随机整数。
    //随机用户ID
    val userID = arr1(Random.nextInt(arr1.length))
    //下单时间  将当前系统时间格式化为指定的日期格式，用于后续的业务处理和记录
    val ts = System.currentTimeMillis() //获取当前系统时间的毫秒值，用于生成Timestamp对象和Date对象
    val timestamp = new Timestamp(ts) //将当前系统时间的毫秒值转化为Timestamp对象，表示一个SQL TIMESTAMP类型的时间。
    val answerTime = sdf.format(new Date(ts)) //将当前系统时间的毫秒值转换为Date对象，并将其格式化为指定格式的日期字符串，格式化的规则由变量sdf决定。其中，sdf代表SimpleDateFormat对象，是Java中常用的日期格式化类，用于将时间格式化为指定的格式字符串

    Orders(userID, appIDRandom, cityIDRandom, restaurantIDRandom, food_categoryIDRandom, ordersRandom, ordersScoreRandom, answerTime, timestamp)
  }

  //测试模拟数据
  def main(args: Array[String]): Unit = {
    val printWriter = new PrintWriter(new File("output/order_info.json"))
    val gson = new Gson()
    for (i <- 1 to 2000) {
      println(s"第{$i}条")
      val jsonString = gson.toJson(genOrder())
      println(jsonString)
      //{"user_id":"用户ID_44","app_id":"平台ID_1","city_id":"城市ID_4","restaurant_id":"餐厅ID_3_英语","food_category_id":"菜品ID_chapter_3","order_id":"题目ID_701","score":10,"answer_time":"2020-01-11 17:20:42","ts":"Jan 11, 2020 5:20:42 PM"}

      printWriter.write(jsonString + "\n")
      //Thread.sleep(200)
    }
    printWriter.flush() //将缓冲的输出数据刷新到目标输出流中，但并不关闭PrintWriter对象，可以继续进行输出操作
    printWriter.close() //先调用flush()方法将缓冲区数据刷新到输出流中，然后关闭PrintWriter对象，关闭后不能再进行输出操作，如果再调用输出方法会抛出异常
  }
}