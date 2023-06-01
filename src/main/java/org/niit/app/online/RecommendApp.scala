package org.niit.app.online

import org.niit.common.TApp
import org.niit.controller.RecommendController

/**
 * @作者 YanTianCheng
 * @时间 2023/5/3 22:14
 * @文件: RecommendApp
 * @项目 org.niit.app
 */
object RecommendApp extends App with TApp {
  start("local[*]", "recommend") {
    val recommendController = new RecommendController
    recommendController.dispatch()

  }

}
