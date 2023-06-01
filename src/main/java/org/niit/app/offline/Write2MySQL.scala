package org.niit.app.offline

import org.niit.common.TApp
import org.niit.controller.Write2MySQLController

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : Gary Chen
 * @create 2023/5/25 11:41
 * @desc: 将Kafka中的数据存入MySQL
 */
object Write2MySQL extends App with TApp {
  start("local[*]", "Writetomysql") {

    val writetomysql = new Write2MySQLController
    writetomysql.dispatch()
  }


}
