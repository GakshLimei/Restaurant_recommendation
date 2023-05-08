package org.niit.service

import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders


class RESDataService  {

  def dataAnalysis(top:DStream[Orders]):Unit={


  }

  //统计Top10热门外卖菜品（次数累加排序）
  private def popularDishesTop10(top:DStream[Orders]):Unit={

    //k-v (菜品，1)
    val mapDS=top.map(data=>{
      (data.food_category_id,1)
    })
    //
    

  }

}
