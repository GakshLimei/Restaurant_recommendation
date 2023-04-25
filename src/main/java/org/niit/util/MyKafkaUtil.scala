package org.niit.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 10:52
 * @文件: MyKafkaUtil
 * @项目 org.niit.util
 */
object MyKafkaUtil {

  var kafkaParam = Map(
    "bootstrap.servers" -> "node1:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest 自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "earliest",
    //如果是 true，则这个消费者的偏移量会在后台自动提交,但是 kafka 宕机容易丢失数据
    //如果是 false，会需要手动维护 kafka 偏移量
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(groupId: String, topic: String, ssc: StreamingContext):
  InputDStream[ConsumerRecord[String, String]] = {

    kafkaParam = kafkaParam ++ Map("group.id" -> groupId)

    val dStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String,
          String](Array(topic), kafkaParam))
    dStream
  }

  //这段代码定义了一个名为 getKafkaStream 的函数，其返回类型是 InputDStream[ConsumerRecord[String, String]]，用于创建 Kafka 流并将其用于流式处理。
  //该函数的参数包括 groupId，topic 和 ssc。
  //该函数内部创建了一个 kafkaParam 对象，该对象包含了 groupId 参数的键值对，并将其添加到了一个 Map 对象中。
  //然后，该函数使用 KafkaUtils.createDirectStream 方法创建了一个 Kafka 直接流，并将其用于流式处理。
  // 该方法的参数包括 ssc，LocationStrategies.PreferConsistent，表示该流应使用位于 Kafka 集群中心的位置策略，
  // ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)，表示该流应使用订阅策略来订阅 Kafka 主题，
  // 并将 kafkaParam 对象作为参数传递给该策略。
  //最后，该函数返回创建的 Kafka 直接流对象。

}
