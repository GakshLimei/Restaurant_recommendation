package org.niit.app

import org.niit.common.TApp
import org.niit.controller.RealTimeAnlsController

/**
 * @author: Jielong
 * @Created: 2023/4/29 17:54
 * @desc: 实时统计
 */
object RealTimeAnalyseApp extends App with TApp {
  start("local[*]", "MovieData") {
    val realTimeAnlsController = new RealTimeAnlsController
    realTimeAnlsController.dispatch()

  }
}
