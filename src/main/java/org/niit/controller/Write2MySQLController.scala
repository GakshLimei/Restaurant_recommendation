package org.niit.controller

import org.niit.handler.DataHandler
import org.niit.service.Write2MySQLService

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : Gary Chen
 * @create 2023/5/25 11:53
 */
class Write2MySQLController {
  private val write2mysql = new Write2MySQLService()

  def dispatch(): Unit = {
    val orderData = DataHandler.kafkaOrdersDatHandler("Consumer_group1", "takeaway")
    write2mysql.writetomysql(orderData)

    DataHandler.startAndAwait() //等待kafka关闭采集
  }

}
