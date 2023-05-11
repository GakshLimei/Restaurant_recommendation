package org.niit.service


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders

class RealTimeAnalyse {
  def dataAnalysis(orders: DStream[Orders]): Unit = {
    hotCuisineTop10(orders)
    hotCanteenTop2(orders)
    hotPlatformTop3(orders)
Thread.sleep(20000)
  }

  //Top10前十个用户最喜欢的菜品（用户评分）
  private def hotCuisineTop10(orders: DStream[Orders]): Unit = {
    val mapDS = orders.map(data => {
      (data.food_category_id, data.score)
    })
    //    val reducerData = mapDS.reduceByKey(_ + _)
    mapDS.foreachRDD(rdd => {
      val sortRDD = rdd.sortBy(_._2, false)
      val top10 = sortRDD.take(10)
      println("---------Top10前十个用户最喜欢的菜品（用户评分）---------")
      top10.foreach(println)
    })
  }
  //根据时间段统计餐厅下单量
  private def hotCanteenTop2(orders: DStream[Orders]): Unit = {
    val mapDS = orders.map(data => {
      (data.restaurant_id, data.order_time)
    })
    mapDS.foreachRDD(rdd => {
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
      println("---------根据时间段统计餐厅下单量---------")
      println("----------清晨-餐厅下单量TOP20---------")
      morningRDD.take(20).foreach(println)
      println(s"清晨餐厅下单总数: ${morningRDD.count()}")
      println("----------早上-餐厅下单量TOP20---------")
      earlyMorningRDD.take(20).foreach(println)
      println(s"早上餐厅下单总数: ${earlyMorningRDD.count()}")
      println("----------中午-餐厅下单量TOP20---------")
      noonRDD.take(20).foreach(println)
      println(s"中午餐厅下单总数: ${noonRDD.count()}")
      println("----------晚上-餐厅下单量TOP20---------")
      eveningRDD.take(20).foreach(println)
      println(s"晚上餐厅下单总数: ${eveningRDD.count()}")
    })
  }


  //根据时间段统计平台销量
  private def hotPlatformTop3(orders: DStream[Orders]): Unit = {
    val mapDS = orders.map(data => {
      (data.app_id, data.order_time)
    })
    mapDS.foreachRDD(rdd => {
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
    })
  }

}
