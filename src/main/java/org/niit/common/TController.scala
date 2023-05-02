package org.niit.common

import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.AdClickData

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 17:08
 * @文件: TController
 * @项目 org.niit.common
 */
trait TController {
  def dispatch(data:DStream[AdClickData]):Unit

}
