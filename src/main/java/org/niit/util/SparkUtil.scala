package org.niit.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.niit.util.SparkUtil.{putSC, putSSC, putSpark}

/**
 * @作者 YanTianCheng
 * @时间 2023/4/21 13:56
 * @文件: SparkUtil
 * @项目 org.niit.util
 */
object SparkUtil {
  private val scLocal = new ThreadLocal[SparkContext]   //专门存储SC的线程池
  private val sparkLocal = new ThreadLocal[SparkSession]
  private val sscLocal = new ThreadLocal[StreamingContext]
  private var sc: SparkContext = _;
  private var seconds: Duration = _;
  private var ssc: StreamingContext = _;
  private var spark: SparkSession = _;

  def CreateSpark(sparkConf: SparkConf, seconds: Duration = Seconds(10)): SparkContext = {

    if (sc == null) {
      spark = SparkSession.builder().config(sparkConf).getOrCreate()
      sc = spark.sparkContext
      putSC(sc)
      putSpark(spark)

      if (this.seconds != seconds) {
        sscLocal.remove()
        ssc = new StreamingContext(sc, seconds)
        this.seconds = seconds
      };
      putSSC(ssc)
    }
    sc
  }

  def getOrCreateStreamingContext(sparkContext: SparkContext, seconds: Duration): StreamingContext = {


    if (this.seconds != seconds) {
      sscLocal.remove()
      ssc = new StreamingContext(sparkContext, seconds)
      this.seconds = seconds
      putSSC(ssc)
    }
    ssc
  }

  private def putSC(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }


  private def putSpark(spark: SparkSession): Unit = {
    sparkLocal.set(spark)
  }

  private def putSSC(ssc: StreamingContext): Unit = {
    sscLocal.set(ssc)
  }

  def takeSC(): SparkContext = {
    scLocal.get()
  }

  def takeSpark(): SparkSession = {
    sparkLocal.get()
  }

  def takeSSC(): StreamingContext = {
    sscLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
    sparkLocal.remove()
    sscLocal.remove()
  }
  //函数体内有三个语句，它们的作用是从当前线程的本地变量中删除名为"scLocal"、"sparkLocal"和"sscLocal"的变量。
  //分布式计算框架Apache Spark相关的本地变量。本地变量是在当前线程中定义的变量，只能在该线程中访问和修改。
  //删除这些本地变量可能是为了避免内存泄漏或其他问题，或者为了确保每个线程都有自己的实例，以避免线程安全问题。

}
