package org.niit.controller

import org.niit.handler.DataHandler
import org.niit.service.RecommendService

/**
 * @作者 YanTianCheng
 * @时间 2023/5/3 21:47
 * @文件: RecommendController
 * @项目 org.niit.controller
 */
class RecommendController {
  private val recommendService = new RecommendService

  def dispatch(): Unit = {
    val ordered = DataHandler.kafkaOrdersDatHandler("BD2","tak")
    recommendService.dataAnalysis(ordered)
    DataHandler.startOrders()
  }

}
