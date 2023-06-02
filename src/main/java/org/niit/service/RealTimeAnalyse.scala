package org.niit.service


import org.apache.spark.rdd
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, dense_rank, desc}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders
import org.niit.util.SparkUtil

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

class RealTimeAnalyse {

  val spark = SparkUtil.takeSpark()
  import spark.implicits._
  val url = "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8"
  val dbProperties = new Properties()
  dbProperties.setProperty("user", "root")
  dbProperties.setProperty("password", "Niit@123")
  dbProperties.setProperty("driver", "com.mysql.jdbc.Driver")
  def dataAnalysis(orders: DStream[Orders]): Unit = {
//    hotCuisineTop10(orders)
//    analyseByTime(orders)
    hotPlatformByTimeTop3(orders)
  }


  def hotCuisineTop10(orders: DStream[Orders]) = {
    val windowedOrders = orders.window(Seconds(10), Seconds(10))

    // 先按照菜品名和评分进行分组，统计每个菜品的平均评分
    windowedOrders.foreachRDD(rdd=>{
      val ordersDF = rdd.toDF()
      val foodAvgScoreDF = ordersDF
      .groupBy($"food_category")
      .agg(avg($"score").as("avg_score"))


    // 再按照菜品进行分组，统计每个菜品的平均评分排名
    val foodRankDF = foodAvgScoreDF
      .withColumn("rank", dense_rank().over(Window.orderBy(desc("avg_score"))))
      .filter($"rank" <= 10)


    // 通过关联获取菜品信息
    println("---------Top10前十个用户最喜欢的菜品（用户评分）---------")
    val rids = ordersDF.join(foodRankDF, Seq("food_category"))
    rids.select($"food_category", $"avg_score" ,$"app_name",$"rank")
      .distinct()
      .orderBy("rank")
      .limit(10)
      .show()

    val res = rids.select($"food_category", $"avg_score" ,$"app_name",$"rank")
    res.write.mode(SaveMode.Overwrite)
      .jdbc(url, "RT_top10_cuisines", dbProperties)
  })
  }

  //根据时间段统计平台销量
  private def hotPlatformByTimeTop3(orders: DStream[Orders]): Unit = {

    val mapDS = orders.map(data => {
      (data.app_name, data.order_time,data.restaurant_id)
    })
    val windowedOrders = mapDS.window(Seconds(10), Seconds(10))
    windowedOrders.foreachRDD(rdd => {
      val ordersDF =  rdd.toDF("app_name","order_time","restaurant_id")
      ordersDF.write.mode(SaveMode.Overwrite)
        .jdbc(url,"RT_hotCanteenTop3",dbProperties)
//      ordersDF.show()
      // 对 RDD 进行处理，将时间段划分为早上、中午、下午和晚上
      val timePeriodRDD = rdd.map(record => {
        val timestampStr = record._2
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val timestamp = LocalDateTime.parse(timestampStr, formatter)
        val hourOfDay = timestamp.getHour
        if (hourOfDay < 6) {
          ("清晨", record)
        } else if (hourOfDay < 12) {
          ("早上", record)
        } else if (hourOfDay < 18) {
          ("中午", record)
        } else {
          ("晚上", record)

        }
      })
      val morningRDD = timePeriodRDD.filter(record => record._1 == "清晨").sortBy(_._2, false)
      val earlyMorningRDD = timePeriodRDD.filter(record => record._1 == "早上").sortBy(_._2, false)
      val noonRDD = timePeriodRDD.filter(record => record._1 == "中午").sortBy(_._2, false)
      val eveningRDD = timePeriodRDD.filter(record => record._1 == "晚上").sortBy(_._2, false)
      println("---------根据时间段统计平台销量---------")
      println("----------清晨-平台下单量TOP20---------")
      morningRDD.take(20).foreach(println)
      println(s"清晨平台销量总数: ${morningRDD.count()}")
      println("----------早上-平台下单量TOP20---------")
      earlyMorningRDD.take(20).foreach(println)
      println(s"早上平台销量总数: ${earlyMorningRDD.count()}")
      println("----------中午-平台下单量TOP20---------")
      noonRDD.take(20).foreach(println)
      println(s"中午平台销量总数: ${noonRDD.count()}")
      println("----------晚上-平台下单量TOP20---------")
      eveningRDD.take(20).foreach(println)
      println(s"晚上平台销量总数: ${eveningRDD.count()}")
      // 将 RDD 转成 DataFrame
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._


      val orderStatsDF = timePeriodRDD.toDF("time", "order_time")
      println("---------根据时间段统计平台下单量test---------")
      orderStatsDF.show

      // 分组操作，统计每个时间段内每个餐厅的订单数
      val countResult: DataFrame = orderStatsDF.groupBy($"time").count()
      // 将结果输出到 MySQL 数据库中

    })
  }
}
