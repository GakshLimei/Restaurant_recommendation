package org.niit.controller

import org.niit.service.BatchDataService

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 11:02
 * @desc:离线服务控制层
 */
class BatchDataAppController {
  //  离线分析
  private val batchDataService = new BatchDataService()

  def dispatch(): Unit = {

    batchDataService.dataAnalysis()
    batchDataService.recommendService()


  }
}
