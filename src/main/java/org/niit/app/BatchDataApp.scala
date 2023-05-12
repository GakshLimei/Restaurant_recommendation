package org.niit.app

import org.niit.common.TApp
import org.niit.controller.BatchDataAppController

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 11:02
 * @desc:
 */
object BatchDataApp extends App with TApp {
  start("local[*]", "BatchDataApp") {
    val batchDataController = new BatchDataAppController()

    batchDataController.dispatch()


  }


}