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

  def startOrders(): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }

}
