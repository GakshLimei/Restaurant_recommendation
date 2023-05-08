package org.niit.app

import org.niit.common.TApp
import org.niit.controller.RESDataController

object RESDataApp extends App with TApp{
  start("local[*]", "resData") {

    val resDataContoller = new RESDataController
    resDataContoller.dispatch()
  }
}
