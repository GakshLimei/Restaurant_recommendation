package org.niit.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import org.niit.bean.Orders
import org.niit.util.SparkUtil

/**
 *
 * @author: Gary Chen
 * @Created: 2023/5/25 11:55
 * @desc: Write2MySQL服务层
 *
 */
class Write2MySQLService() {
  val spark = SparkUtil.takeSpark()

  import spark.implicits._

  def writetomysql(orders: DStream[Orders]): Unit = {
    orders.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val ordersDF = rdd.toDF("user_id", "app_name", "city_name", "restaurant_id", "food_category", "order_id", "score", "order_time", "ts")
        ordersDF.write
          .format("jdbc")
          .option("url", "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", "history_orders")
          .option("user", "root")
          .option("password", "Niit@123")
          .option("batchsize", "1000") // 指定批量写入大小
//          .option("createTablePrimaryKey", "order_id")
//          .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='订单表' PRIMARY KEY (order_id)")
          .option("createTableColumnTypes", "user_id varchar(10), app_name varchar(10) , city_name varchar(10), restaurant_id varchar(10), food_category varchar(10) , order_id varchar(10)")
          .mode(SaveMode.Append)
          .save()
        println("Data successfully written to MySQL database.")
      }
    })
  }

}
