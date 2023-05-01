package org.niit.mock

import java.util.Properties
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.google.gson.Gson
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

/**
 * @作者 YanTianCheng
 * @时间 2023/5/1 13:24
 * @文件: KafkaProducerTest
 * @项目 org.niit.mock
 */
/**
 * 使用线程池调度Kafka生产者发送任务,将数据实时发送到Kafka
 */
object KafkaProducerTest {
  def main(args: Array[String]): Unit = {
    //创建线程池
    val threadPoolExecutor:ThreadPoolExecutor = new ThreadPoolExecutor(
    5,  //活跃线程数
    10,  //最大线程数
    5,    //最大空闲时间
    TimeUnit.SECONDS,   //时间单位
    new ArrayBlockingQueue[Runnable](10))  //任务等待队列,未被调度的线程任务,会在该队列中排队
    //提交任务
    for (i <- 1 to 4){
      threadPoolExecutor.submit(new KafkaProducerThread)
    }
  }//for循环指定了启动线程池中线程的数量，每次循环都会创建一个名为KafkaProducerThread的线程，然后将线程提交到线程池中
}

/**
 * 发送数据到kafka的生产者线程对象
 */
class KafkaProducerThread extends Thread{
  //通过LoggerFactory类创建一个日志记录器，用于记录程序运行时产生的日志
  val logger = LoggerFactory.getLogger(classOf[KafkaProducerThread])

  val props = new Properties()    //创建了一个Properties对象，用于保存Kafka生产者的配置参数
  props.setProperty("bootstrap.servers", "node1:9092")  //Kafka服务的地址
  props.setProperty("ack", "1")   //消息确认机制
  props.setProperty("batch.size", "16384")  //批处理大小
  props.setProperty("linger.ms", "5")      //延迟提交时间
  props.setProperty("buffer.memory", "33554432")   //缓存大小
  props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //序列化key
  props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")  //序列化valuer

  //创建了一个KafkaProducer实例对象，并通过传入String类型的键和值的序列化器来设置消息的格式化方式
  val producer:KafkaProducer[String,String] = new KafkaProducer[String,String](props)
  //使用Google的Gson库创建一个Gson对象，用于将消息对象序列化成JSON格式的字符串
  val gson = new Gson()

  override def run(): Unit = {
    while (true){
      //模拟数据产生
      val orders = Simulator.getQuestion()
      //将产生的模拟数据转换成JSON 以JSON的格式存储在Kafka
      val jsonString = gson.toJson(orders)

      producer.send(new ProducerRecord[String,String]("edu2",jsonString), new Callback {
        override def onCompletion(metadata: RecordMetadata,exception: Exception): Unit = {
          if (exception == null){
            println("当前分区-偏移量：" + metadata.partition() + "-" + metadata.offset() + "\n数据发送成功：" +jsonString)
            logger.info("当前分区-偏移量：" + metadata.partition() + "-" + metadata.offset() + "\n数据发送成功：" + jsonString)
          }else {
            logger.error("数据发送失败：" +exception.getMessage)
          }

        }
      })
      Thread.sleep(300)
    }
  }
}
