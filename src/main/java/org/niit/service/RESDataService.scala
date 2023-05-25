package org.niit.service

import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders




class RESDataService  {

  def dataAnalysis(top:DStream[Orders]):Unit={

    popularDishesTop10(top)
    takeawaySalesCitiesTop10(top)
    popularRestaurantsTop5(top)

  }

  //统计Top10热门外卖菜品（次数累加排序）
  private def popularDishesTop10(top:DStream[Orders]):Unit={

    //k-v (外卖菜品，1)
    val mapDS=top.map(data=>{
      (data.food_category_id,1)
    })
    //
    val reduceData=mapDS.reduceByKey(_ + _)


    reduceData.foreachRDD(rdd=>{
      //第一个下划线：(外卖菜品，3)     _2:次数 元组索引值是从1开始 默认是降序
      val sortRDD:RDD[(String,Int)] = rdd.sortBy(_._2,false)
      val top10:Array[(String,Int)] =sortRDD.take(10)
      println("------------统计Top10热门外卖菜品----------------")
      top10.foreach(println)

   // 将 RDD 转成 DataFrame
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val resultDF = spark.createDataset(top10).toDF("categoryID", "number")
      resultDF.createOrReplaceTempView("categoryTop10")

      val url = "jdbc:mysql://localhost:3306/takeaway?useUnicode=true&characterEncoding=utf8"
      val user = "root"
      val password = "root"

      resultDF.orderBy($"number".desc).write.mode(SaveMode.Append).jdbc(url, "categoryTop10", new java.util.Properties() {
        {
          setProperty("driver", "com.mysql.cj.jdbc.Driver")
          setProperty("user", user)
          setProperty("password", password)
        }
      })





    })



  }

  //外卖销量Top10城市（下单量）

  private def takeawaySalesCitiesTop10(top:DStream[Orders]):Unit={

    val mapDS=top.map(data=>{
      (data.city_id,1)
    })
    //
    val reduceData=mapDS.reduceByKey(_ + _)

    reduceData.foreachRDD(rdd=>{
      val sortRDD=rdd.sortBy(_._2,false)
      val top10=sortRDD.take(10)
      println("------------统计top10外卖销量城市------------------")
      top10.foreach(println)


      // 将 RDD 转成 DataFrame
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val resultDF = spark.createDataset(top10).toDF("citiesID", "number")
      resultDF.createOrReplaceTempView("citiesTop10")

      val url = "jdbc:mysql://localhost:3306/takeaway?useUnicode=true&characterEncoding=utf8"
      val user = "root"
      val password = "root"

      resultDF.orderBy($"number".desc).write.mode(SaveMode.Append).jdbc(url, "citiesTop10", new java.util.Properties() {
        {
          setProperty("driver", "com.mysql.cj.jdbc.Driver")
          setProperty("user", user)
          setProperty("password", password)
        }
      })


    })
  }


  //热门餐厅Top5

  private def popularRestaurantsTop5(top:DStream[Orders]):Unit={

    val mapDS=top.map(data=>{
      (data.restaurant_id,1)
    })

    val reduceData=mapDS.reduceByKey(_ + _)

    reduceData.foreachRDD(rdd=>{
      val sortRDD=rdd.sortBy(_._2,false)
      val top5=sortRDD.take(5)
      println("------------热门餐厅Top5------------------")
      top5.foreach(println)


      // 将 RDD 转成 DataFrame
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val resultDF = spark.createDataset(top5).toDF("restaurant_ID", "number")
      resultDF.createOrReplaceTempView("restaurantsTop5")

      val url = "jdbc:mysql://localhost:3306/takeaway?useUnicode=true&characterEncoding=utf8"
      val user = "root"
      val password = "root"

      resultDF.orderBy($"number".desc).write.mode(SaveMode.Append).jdbc(url, "restaurantsTop5", new java.util.Properties() {
        {
          setProperty("driver", "com.mysql.cj.jdbc.Driver")
          setProperty("user", user)
          setProperty("password", password)
        }
      })




    })
  }


}
