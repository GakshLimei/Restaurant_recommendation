package org.niit.common

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.niit.util.SparkUtil

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 17:01
 * @文件: TApp
 * @项目 org.niit.common
 */
trait TApp {
  def start(master:String = "local[*]",appName:String = "application")(op : => Unit) {
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = SparkUtil.CreateSpark(sparkConf,Seconds(5))
    val spark = SparkUtil.takeSpark()
    val ssc = SparkUtil.takeSSC()
    sc.setLogLevel("ERROR")



    try{
      op
    }catch {
      case ex => ex.printStackTrace()
    }
    spark.stop()
    ssc.stop(true,true)
    sc.stop()
    SparkUtil.clear()

  }

}
