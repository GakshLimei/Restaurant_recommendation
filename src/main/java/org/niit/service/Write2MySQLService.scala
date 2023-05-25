package org.niit.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders
import org.niit.util.SparkUtil

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : Gary Chen
 * @create 2023/5/25 11:55
 */
class Write2MySQLService() {
      val spark = SparkUtil.takeSpark()
      import spark.implicits._
  def writetomysql(orders: DStream[Orders]): Unit = {
    orders.foreachRDD(rdd => {

      val ordersDF = rdd.toDF()

      ordersDF.write
        .format("jdbc")
        .option("url", "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "orders")
        .option("user", "root")
        .option("password", "Niit@123")
        .mode(SaveMode.Append)
        .save()
      println("Data successfully written to MySQL database.")
    })
  }

}
