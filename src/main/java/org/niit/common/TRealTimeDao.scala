package org.niit.common

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 17:22
 * @文件: TRealTimeDao
 * @项目 org.niit.common
 */
trait TRealTimeDao {
  def insertRealTimeAd(day: String, area: String, city: String, ad: String, count: String): Unit
}
