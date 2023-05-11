package org.niit.controller

import org.niit.handler.DataHandler
import org.niit.service.BatchDataService

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 11:02
 * @desc:
 */
class BatchDataAppController {
  //  离线分析
  private val batchDataService = new BatchDataService()

  def dispatch(): Unit = {
//    val orderData = DataHandler.kafkaOrdersDatHandler("BD2", "edu2")
    batchDataService.dataAnalysis()
//    DataHandler.startAndAwait()
    /*
      1.为什么实时分析（eduDataService，eduRecommendService）需要传递answerData
       离线分析（eduBatchService）不需要传递参数？

       实时分析数据：来源于Kafka。用户的每一个操作是存放在日志文件（data.txt）-->Flume-->Kafka-->Spark
          Spark和Kafka之间的数据传递，依赖Spark Streaming
           1.Spark 和 Kafka建立连接
           2.编写数据转换规则
           3.开启采集并等待数据接收
           4.Spark Streaming 必须要有输出  foreachRdd 或者print

       2.为什么 用离线分析的时候 需要将 DataHandler.startAndAwait() 屏蔽
          离线分析是将数据库（MySQL）中存在的历史数据，拿出来进行分析的。和Kafka没有任何关系
          按照实际项目来说 实时分析和离线分析应该是两个App,并且离线分析是通过定时器去执行的，一般是在凌晨00：00后去执行。
          因为是对前一天的历史数据进行的分析


       3.在这个项目中HBase用来干嘛的?
          存放模型路径的
          因为在后面一旦要升级模型的话，不需要将正在运行的Spark停掉去修改模型路径
          而是通过HBase存放的路径即可更新

       */
  }
}
