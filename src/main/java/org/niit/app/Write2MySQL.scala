package org.niit.app

import org.niit.common.TApp
import org.niit.controller.Write2MySQLController

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : Gary Chen
 * @create 2023/5/25 11:41
 */
object Write2MySQL extends App with TApp {
  start("local[*]", "Writetomysql") {

    val writetomysql = new Write2MySQLController
    writetomysql.dispatch()
  }


}
