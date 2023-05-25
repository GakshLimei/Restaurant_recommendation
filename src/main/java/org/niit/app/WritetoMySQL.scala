package org.niit.app

import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders
import org.niit.common.TApp
import org.niit.controller.WriteToController
import org.niit.handler.DataHandler

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : Gary Chen
 * @create 2023/5/25 11:41
 */
object WritetoMySQL extends App with TApp{
  start("local[*]", "Writetomysql") {

    val writetomysql = new WriteToController
    writetomysql.dispatch()
  }




}
