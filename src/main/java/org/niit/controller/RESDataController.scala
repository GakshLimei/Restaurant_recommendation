package org.niit.controller

import org.niit.handler.DataHandler
import org.niit.service.{RESDataService, RecommendService}

class RESDataController {

  private val resDataService=new RESDataService()
  private val resReccommendService=new RecommendService()

  def dispatch():Unit={
    val restaurantData=DataHandler.kafkaOrdersDatHandler("BD2","res1")
//    resDataService.

      DataHandler.startOrders()
  }

}
