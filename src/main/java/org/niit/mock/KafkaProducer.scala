package org.niit.mock

import com.google.gson.{Gson, GsonBuilder}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.niit.bean.Orders
import org.niit.util.{JDBCUtil, MyKafkaUtil}
import org.slf4j.LoggerFactory

import java.sql.{Connection, Timestamp}
import java.time.Duration
import java.util
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.{Collections, Properties}

/**
 * @author: Gary Chen
 * @Created: 2023/4/28 13:41
 * @desc: kafka生产者和消费者
 */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    //创建线程池
    val threadPoolExecutor: ThreadPoolExecutor = new ThreadPoolExecutor(5, //活跃线程数
      10, //最大线程数
      3, //最大空闲时间
      TimeUnit.SECONDS, //时间单位
      new ArrayBlockingQueue[Runnable](10)) //任务等待队列,未被调度的线程任务,会在该队列中排队
    //提交任务
    for (i <- 1 to 12) {
      threadPoolExecutor.submit(new KafkaProducerThread)
    }
    //启动一个新线程读取数据并将其写入MySQL表
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          val t = new Thread(new KafkaConsumerThread())
          t.start()
          t.join()
          Thread.sleep(5000)
        }
      }
    }).start()
  }
}

/**
 * 发送数据到kafka的生产者线程对象
 */
class KafkaProducerThread extends Thread {

  val logger = LoggerFactory.getLogger(classOf[KafkaProducerThread])
  val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer
  val gson = new Gson()

  override def run(): Unit = {
    println("***********************************************************************************************************" +
      "**********************************************************************************************************")
    while (true) {
      //生成模拟数据
      val question = Simulator.genOrder()
      //将生成的模拟数据 转换 为JSON
      val jsonString = gson.toJson(question)

      //并存储到Kafka
      producer.send(new ProducerRecord[String, String]("takeaway", jsonString), new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            println("当前分区-偏移量：" + metadata.partition() + "-" + metadata.offset() + "\n数据发送成功：" + jsonString)
            logger.info("当前分区-偏移量：" + metadata.partition() + "-" + metadata.offset() + "\n数据发送成功：" + jsonString)
          } else {
            logger.error("数据发送失败：" + exception.getMessage)
          }
        }
      })
      Thread.sleep(300)
    }
  }
}

/**
 * 从kafka中读取数据，写入到MySQL中
 */
class KafkaConsumerThread extends Thread {
  override def run(): Unit = {

    val groupId = "T2"
    val consumer = MyKafkaUtil.getKafkaConsumer(groupId)
    consumer.subscribe(Collections.singletonList("takeaway"))
    val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(5))
    val conn: Connection = JDBCUtil.getConnection
    val gson: Gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
    val iterator: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
    println("***********************************************************************************************************" +
      "**********************************************************************************************************")
    while (iterator.hasNext) {
      val record: ConsumerRecord[String, String] = iterator.next()
      val orderJson: String = record.value()
      println("将以下数据保存到 MySQL 中：")
      println(orderJson)
      val orders: Orders = gson.fromJson(orderJson, classOf[Orders])
      val orderTime: String = orders.order_time
      val ts: Timestamp = orders.ts
      val tableName = "history_orders"
      val tableSchema =
        "`user_id` VARCHAR(10) NULL, " +
          "`app_name` VARCHAR(10) NULL, " +
          "`city_name` VARCHAR(10) NULL, " +
          "`restaurant_id` VARCHAR(10) NULL, " +
          "`food_category` VARCHAR(10) NULL, " +
          "`order_id` VARCHAR(10) NOT NULL, " +
          "`score` DOUBLE NOT NULL, " +
          "`order_time` TEXT NULL, " +
          "`ts` TIMESTAMP NULL, " +
          "PRIMARY KEY (`order_id`)"

      JDBCUtil.createTableIfNotExists(conn, tableName, tableSchema)

      val sql: String =
        s"""
           |
           |INSERT INTO `Takeaway`.`history_orders`
           |(user_id, app_name, city_name, restaurant_id, food_category, order_id, score, order_time, ts)
           |VALUES (
           |  '${orders.user_id}', '${orders.app_name}', '${orders.city_name}', '${orders.restaurant_id}',
           |  '${orders.food_category}', '${orders.order_id}', ${orders.score}, '$orderTime', '$ts'
           |)
           |""".stripMargin

      JDBCUtil.executeUpdate(conn, sql)

      consumer.commitSync()
    }
    consumer.close()
    //    statement.close()
    conn.close()
  }
}