package org.niit.mock

import com.google.gson.Gson
import org.niit.bean.Orders

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
 * 外卖数据模拟程序
 */

object Simulator {
  //模拟数据
  //用户ID
  val arr1 = ArrayBuffer[Long]() //初始化为一个空的 ArrayBuffer 类型的数组
  for (i <- 1 to 2000) {
    arr1 += i //将每个元素的值设置为字符串 "用户ID_" + i
  }
  //平台ID
  val arr2 = Array("美团", "饿了么", "百度外卖")
  //城市ID
  val arr3 = Array("城市ID_1", "城市ID_2", "城市ID_3", "城市ID_4", "城市ID_5", "城市ID_6", "城市ID_7", "城市ID_8", "城市ID_9", "城市ID_10")
  //餐厅ID
  val arr4 = ArrayBuffer[Long]() //初始化为一个空的 ArrayBuffer 类型的数组
  for (i <- 1 to 1000) {
    arr4 += i //将每个元素的值设置为字符串 "餐厅ID_" + i
  }
  //菜品ID
  val arr5 = Array("热菜", "凉菜", "汤类", "火锅", "快餐", "烧烤", "粉面", "披萨意面", "包子馒头", "特色菜")

  //订单ID与平台、城市、餐厅、菜品的对应关系,
  val ordersMap = collection.mutable.HashMap[String, ArrayBuffer[Long]]()
  val restaurantMap = collection.mutable.HashMap[String, ArrayBuffer[String]]()
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  var ordersID = 1
  for (appID <- arr2; cityID <- arr3; restaurantID <- arr4; food_categoryID <- arr5) {
    val key = new StringBuilder()
      .append(appID).append("^")
      .append(cityID).append("^")
      .append(restaurantID).append("^")
      .append(food_categoryID)

    val ordersArr = ArrayBuffer[Long]()
    val restaurantArr = ArrayBuffer[String]()
    for (i <- 1 to 20) {
      ordersArr += ordersID

      ordersID += 1

    }
    ordersMap.put(key.toString(), ordersArr)
    restaurantMap.put(key.toString(), restaurantArr)
  }
  var restaurantID = 1

  //测试模拟数据
  def main(args: Array[String]): Unit = {
    val printWriter = new PrintWriter(new File("output/order_info.json"))
    val gson = new Gson()
    for (i <- 1 to 300000) {
      println(s"第{$i}条")
      val jsonString = gson.toJson(genOrder())
      println(jsonString)
      //{"user_id":"用户ID_44","app_id":"平台ID_1","city_id":"城市ID_4","restaurant_id":"餐厅ID_3","food_category_id":"菜品ID_category_3","order_id":"题目ID_701","score":10,"order_time":"2020-01-11 17:20:42","ts":"Jan 11, 2020 5:20:42 PM"}

      printWriter.write(jsonString + "\n")
      //Thread.sleep(200)
    }
    printWriter.flush() //将缓冲的输出数据刷新到目标输出流中，但并不关闭PrintWriter对象，可以继续进行输出操作
    printWriter.close() //先调用flush()方法将缓冲区数据刷新到输出流中，然后关闭PrintWriter对象，关闭后不能再进行输出操作，如果再调用输出方法会抛出异常
  }

  def genOrder() = {
    //随机平台ID
    val appIDRandom = arr2(Random.nextInt(arr2.length))
    //随机城市ID
    val cityIDRandom = arr3(Random.nextInt(arr3.length))
    //随机餐厅ID
    val restaurantIDRandom = arr4(Random.nextInt(arr4.length))
    //随机菜品ID
    val food_categoryIDRandom = arr5(Random.nextInt(arr5.length))


    val cityMap = Map(
      "城市ID_1" -> "崇左",
      "城市ID_2" -> "来宾",
      "城市ID_3" -> "钦州",
      "城市ID_4" -> "防城港",
      "城市ID_5" -> "南宁",
      "城市ID_6" -> "柳州",
      "城市ID_7" -> "桂林",
      "城市ID_8" -> "北海",
      "城市ID_9" -> "玉林",
      "城市ID_10" -> "百色"
    )


    val key = new StringBuilder()
      .append(appIDRandom).append("^")
      .append(cityIDRandom).append("^")
      .append(restaurantIDRandom).append("^")
      .append(food_categoryIDRandom)

    //取出订单
    val ordersArr = ordersMap(key.toString())
    //随机订单ID
    val ordersIDRandom = ordersArr(Random.nextInt(ordersArr.length))
    //随机订单评分扣分
    val ordersScoreRandom = Random.nextInt(11) + 1 //Kotlin中的Random类的方法，表示生成一个0到10（不包含10）之间的随机整数。
    //随机用户ID
    val userID = arr1(Random.nextInt(arr1.length))

    val city = cityMap.getOrElse(cityIDRandom, "")

    //下单时间  将当前系统时间格式化为指定的日期格式，用于后续的业务处理和记录
    // 指定日期范围
    val startDate = "2022-01-01"
    val endDate = "2022-12-31"

    // 生成随机时间
    val random = new Random()
    val start = sdf.parse(s"$startDate 00:00:00").getTime
    val end = sdf.parse(s"$endDate 23:59:59").getTime

    // 转化为毫秒值
    val randomTS = new Timestamp(start + (random.nextDouble() * (end - start)).toLong).getTime
    val timestamp = new Timestamp(System.currentTimeMillis()) //将随机生成的时间的的毫秒值转化为Timestamp对象，表示一个SQL TIMESTAMP类型的时间。
    val orderTime = sdf.format(new Date(randomTS)) //将随机生成的时间的的毫秒值转换为Date对象，并将其格式化为指定格式的日期字符串，格式化的规则由变量sdf决定。其中，sdf代表SimpleDateFormat对象，是Java中常用的日期格式化类，用于将时间格式化为指定的格式字符串


    Orders(userID, appIDRandom, city, restaurantIDRandom, food_categoryIDRandom, ordersIDRandom, ordersScoreRandom, orderTime, timestamp)
  }
}