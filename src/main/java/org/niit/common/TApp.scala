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
  def start(master: String = "local[*]", appName: String = "application")(op: => Unit) {
    // 创建 SparkConf 对象，设置执行模式和应用程序名称
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    // 创建或获取已存在的 SparkContext 对象，并设置时间间隔为 5 秒
    val sc = SparkUtil.CreateSpark(sparkConf, Seconds(5))
    // 获取已存在的 SparkSession 对象
    val spark = SparkUtil.takeSpark()
    // 获取已存在的 StreamingContext 对象
    val ssc = SparkUtil.takeSSC()
    // 设置 SparkContext 的日志级别为 "ERROR"
    sc.setLogLevel("ERROR")

    try {
      // 执行用户定义的操作
      op
    } catch {
      case ex => ex.printStackTrace()
    }

    // 停止 SparkSession、StreamingContext 和 SparkContext 对象
    spark.stop()
    ssc.stop(true, true)
    sc.stop()

    // 清理线程局部变量中存储的 Spark 相关对象
    SparkUtil.clear()
  }
}