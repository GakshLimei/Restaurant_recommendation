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
    //只创建一个消费者和一个消费者组
    //    val data = DataHandler.kafkaAnswerDataHandler("BD2", "edu2")
    //App(value)  --- controller  ---service(value)

    batchDataController.dispatch()


  }


}
