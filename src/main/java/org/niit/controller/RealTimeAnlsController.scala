package org.niit.controller

import org.niit.handler.DataHandler
import org.niit.service.RealTimeAnalyse

class RealTimeAnlsController {
  private val realTimeAnalyse=new RealTimeAnalyse
  def dispatch(): Unit = {
    val answerData = DataHandler.kafkaOrdersDatHandler("BD2", "456")
    realTimeAnalyse.dataAnalysis(answerData)
    DataHandler.startOrders()
  }
}
