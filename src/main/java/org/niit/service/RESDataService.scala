package org.niit.service

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders


class RESDataService  {

  def dataAnalysis(top:DStream[Orders]):Unit={

    popularDishesTop10(top)
    takeawaySalesCitiesTop10(top)

  }

  //统计Top10热门外卖菜品（次数累加排序）
  private def popularDishesTop10(top:DStream[Orders]):Unit={

    //k-v (外卖菜品，1)
    val mapDS=top.map(data=>{
      (data.food_category_id,1)
    })
    //
    val reduceData=mapDS.reduceByKey(_ + _)

    //
    //
    reduceData.foreachRDD(rdd=>{
      //
      val sortRDD:RDD[(String,Int)] = rdd.sortBy(_._2,false)
      val top10:Array[(String,Int)] =sortRDD.take(10)
      println("------------统计Top10热门外卖菜品----------------")
      top10.foreach(println)
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
    })
  }

}
