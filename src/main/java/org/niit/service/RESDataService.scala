package org.niit.service

import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders


class RESDataService {

      //定义数据库连接的相关信息
      val url = "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8"
      val user = "root"
      val password = "Niit@123"
  def dataAnalysis(top: DStream[Orders]): Unit = {

    popularDishesTop10(top)
    takeawaySalesCitiesTop10(top)
    popularRestaurantsTop5(top)

  }

  //统计Top10热门外卖菜品（次数累加排序）
  private def popularDishesTop10(top: DStream[Orders]): Unit = {

    //k-v (外卖菜品，1)
    //使用 map 函数将 top 中的每个元素映射为一个键值对 (data.food_category_id,1)，
    // 其中 data.food_category_id 是外卖菜品的 ID，1 是计数值。
    val mapDS = top.map(data => {
      (data.food_category, 1)
    })

    //使用 reduceByKey 函数将具有相同键的键值对进行聚合，将其值相加。
    val reduceData = mapDS.reduceByKey(_ + _)

    //使用 foreachRDD 函数对结果进行处理。在处理函数中，
    // 首先使用 sortBy 函数对结果进行排序，按照键值对的第二个元素（即计数值）进行降序排序。
    // 然后使用 take 函数取出前 10 个元素，并打印出来。
    reduceData.foreachRDD(rdd => {
      //第一个下划线：(外卖菜品，3)     _2:次数 元组索引值是从1开始 默认是降序
      val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val top10: Array[(String, Int)] = sortRDD.take(10)
      println("------------统计Top10热门外卖菜品----------------")
      top10.foreach(println)

      //将统计结果写入数据库中
      // 将 RDD 转成 DataFrame
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._


      //使用 createDataset 函数将 top10 转换为一个 Dataset，并使用 toDF 函数将其转换为一个 DataFrame，并指定列名为 "categoryID" 和 "number"。
      //然后使用 createOrReplaceTempView 函数创建一个临时视图，名称为 "categoryTop10"。
      val resultDF = spark.createDataset(top10).toDF("categoryID", "number")
      resultDF.createOrReplaceTempView("categoryTop10")


      //使用 orderBy 函数对结果进行排序，按照 "number" 列进行降序排序。然后使用 write 函数将结果写入数据库，模式为 SaveMode.Append，即追加模式。
      //使用 jdbc 函数指定数据库连接信息，并指定表名为 "categoryTop10"。同时，使用一个匿名内部类创建一个 Properties 对象，并设置相关属性。
      resultDF.orderBy($"number".desc).write.mode(SaveMode.Append).jdbc(url, "RTA_categoryTop10", new java.util.Properties() {
        {
          setProperty("driver", "com.mysql.jdbc.Driver")
          setProperty("user", user)
          setProperty("password", password)
        }
      })


    })


  }

  //外卖销量Top10城市（下单量）

  private def takeawaySalesCitiesTop10(top: DStream[Orders]): Unit = {

    val mapDS = top.map(data => {
      (data.city_name, 1)
    })
    //
    val reduceData = mapDS.reduceByKey(_ + _)

    reduceData.foreachRDD(rdd => {
      val sortRDD = rdd.sortBy(_._2, false)
      val top10 = sortRDD.take(10)
      println("------------统计top10外卖销量城市------------------")
      top10.foreach(println)


      // 将 RDD 转成 DataFrame
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val resultDF = spark.createDataset(top10).toDF("citiesID", "number")
      resultDF.createOrReplaceTempView("citiesTop10")


      resultDF.orderBy($"number".desc).write.mode(SaveMode.Append).jdbc(url, "RTA_citiesTop10", new java.util.Properties() {
        {
          setProperty("driver", "com.mysql.jdbc.Driver")
          setProperty("user", user)
          setProperty("password", password)
        }
      })


    })
  }


  //热门餐厅Top5

  private def popularRestaurantsTop5(top: DStream[Orders]): Unit = {

    val mapDS = top.map(data => {
      (data.restaurant_id, 1)
    })

    val reduceData = mapDS.reduceByKey(_ + _)

    reduceData.foreachRDD(rdd => {
      val sortRDD = rdd.sortBy(_._2, false)
      val top5 = sortRDD.take(5)
      println("------------热门餐厅Top5------------------")
      top5.foreach(println)


      // 将 RDD 转成 DataFrame
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val resultDF = spark.createDataset(top5).toDF("restaurant_ID", "number")
      resultDF.createOrReplaceTempView("restaurantsTop5")


      resultDF.orderBy($"number".desc).write.mode(SaveMode.Append).jdbc(url, "RTA_restaurantsTop5", new java.util.Properties() {
        {
          setProperty("driver", "com.mysql.jdbc.Driver")
          setProperty("user", user)
          setProperty("password", password)
        }
      })


    })
  }


}
