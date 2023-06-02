package org.niit.app.offline

import org.niit.common.TApp
import org.niit.controller.BatchDataAppController

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 11:02
 * @desc: 离线服务应用层
 */
// 定义了一个名为 BatchDataApp 的对象，继承自 App trait，并混入了 TApp trait。
object BatchDataApp extends App with TApp {

  // 启动应用程序
  start("local[*]", "BatchDataApp") {
    // 创建 BatchDataAppController 对象
    val batchDataController = new BatchDataAppController()

    // 调用 dispatch() 方法执行具体的应用逻辑
    batchDataController.dispatch()
  }
}
