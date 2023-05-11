package org.niit.app

import org.niit.common.TApp
import org.niit.controller.RealTimeAnlsController

object RealTimeAnalyseApp extends App with TApp{
  start("local[*]","MovieData") {
    val realTimeAnlsController = new RealTimeAnlsController
    realTimeAnlsController.dispatch()

  }
}
