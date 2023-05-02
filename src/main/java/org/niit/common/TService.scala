package org.niit.common

import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.AdClickData

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 17:18
 * @文件: TService
 * @项目 org.niit.common
 */
trait TService {
  def dataAnalysis(data:DStream[AdClickData]):Any

}
