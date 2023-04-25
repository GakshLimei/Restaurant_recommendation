package org.niit.been

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 17:10
 * @文件: AdClickData
 * @项目 org.niit.been
 * 真实环境：日志-->Flume-->Kafka-->Spark-->DB
 * 将 Kafka中模拟的用户点击广告数据封装进AdClickData
 */
case class AdClickData(ts:String, delicacies:String, restaurant:String, user:String,ad:String) extends java.io.Serializable
