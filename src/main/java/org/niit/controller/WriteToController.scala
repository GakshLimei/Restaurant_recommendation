package org.niit.controller

import org.niit.handler.DataHandler
import org.niit.service.Write2MySQLService

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : Gary Chen
 * @create 2023/5/25 11:53
 */
class WriteToController {
  private val write2mysql= new Write2MySQLService()

  def dispatch(): Unit = {
    val orderData = DataHandler.kafkaOrdersDatHandler("BD2", "takeaway")
    write2mysql.writetomysql(orderData)

    DataHandler.startAndAwait() //等待kafka关闭采集
  }

}
