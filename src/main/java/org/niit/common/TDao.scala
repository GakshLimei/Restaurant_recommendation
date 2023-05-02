package org.niit.common

import org.niit.util.SparkUtil

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 17:22
 * @文件: TDao
 * @项目 org.niit.common
 */
trait TDao {
  val sc = SparkUtil.takeSSC()
  def selecBlackUserById(userid:String):Boolean
  def insertBlackList(userid:String):Unit
  def insertUserAdCount(day:String,user:String,ad:String,count:Int):Unit

}
