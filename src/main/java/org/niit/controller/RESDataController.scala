package org.niit.controller

import org.niit.handler.DataHandler
import org.niit.service.RESDataService

class RESDataController {

  private val resDataService = new RESDataService()


  def dispatch(): Unit = {
    val restaurantData = DataHandler.kafkaOrdersDatHandler("BD2", "takeaway")
    resDataService.dataAnalysis(restaurantData)

    DataHandler.startAndAwait() //等待kafka关闭采集
  }

}
