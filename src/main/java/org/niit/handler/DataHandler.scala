package org.niit.handler

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.niit.bean.{AdClickData, Orders}
import org.niit.util.{MyKafkaUtil, SparkUtil}

/**
 * @作者 YanTianCheng
 * @时间 2023/5/3 21:52
 * @文件: DataHandler
 * @项目 org.niit.handler
 */
object DataHandler {
  val ssc = SparkUtil.takeSSC()
  def kafkaAdDataHandler(group:String,topic:String):DStream[AdClickData]={
    val kfData:InputDStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(group,topic,ssc)
    val adClickData:DStream[AdClickData] = kfData.map(kafkaDat =>{
      val data = kafkaDat.value()
      val datas = data.split(" ")
      AdClickData(datas(0),datas(1),datas(2),datas(3),datas(4))
    })
    adClickData
  }
  //调用SparkUtil.takeSSC()方法创建一个Spark Streaming上下文对象ssc，用于接收实时数据流。
  //定义一个名为kafkaAdDataHandler的方法，该方法接受两个参数：group和topic，分别表示Kafka消息队列的消费组和主题。
  //在方法内部调用MyKafkaUtil.getKafkaStream(group,topic,ssc)方法，该方法返回一个InputDStream[ConsumerRecord[String,String]]类型的数据流对象kfData，
  // 表示从Kafka中读取的原始数据. 使用map方法将kfData数据流转换成AdClickData类型的数据流adClickData。
  // 在map函数中，对每条原始数据进行解析，并将解析后的数据封装成AdClickData对象，然后返回该对象。
  //返回adClickData数据流对象。
  //总的来说，这段代码的作用是从Kafka消息队列中读取广告点击数据，然后将数据转换为指定格式的数据流，以便后续进行实时分析和处理。

  def kafkaOrdersDatHandler(group:String,topic:String):DStream[Orders]={
    //从Kafka中获得广告数据
    val kfDataDS:InputDStream[ConsumerRecord[String,String]] = MyKafkaUtil.getKafkaStream(group,topic, ssc)
    val ordersData:DStream[Orders] = kfDataDS.map(kafkaData =>{
      val data = kafkaData.value()
      //JSON --> JavaScr
      val gson = new Gson();
      val orders:Orders = gson.fromJson(data,classOf[Orders])
      orders
    })
    ordersData
  }
  //使用 MyKafkaUtil.getKafkaStream 函数从 Kafka 中获取数据流（DStream）。
  //对于每个从 Kafka 中读取到的消息，使用 Gson 库将其转换为 Orders 对象。
  //将转换后的 Orders 对象组成的 DStream 返回。

  def startOrders(): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }

}
