package org.niit.app

import org.niit.common.TApp
import org.niit.controller.RESDataController

/**
 * @author: 邓立文
 * @Created: 2023/4/28 17:52
 * @desc: 实时分析
 */
object RESDataApp extends App with TApp {
  start("local[*]", "resData") {

    val resDataContoller = new RESDataController
    resDataContoller.dispatch()
  }
}
