package org.niit.dao

import org.apache.spark.sql.Dataset
import org.niit.bean.Orders
import org.niit.util.SparkUtil

import java.util.Properties

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:21
 * @desc:
 */
class BatchDataDao1 {

  private val spark = SparkUtil.takeSpark()

  import spark.implicits._
  //获取takeaway表中的数据
  def getTakeawayData(): Dataset[Orders] = {
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "Niit@123")
    val allInfoDS: Dataset[Orders] = spark.read.jdbc(
      "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8",
      "orders",
      props
    ).as[Orders]
    //返回值
    allInfoDS
  }

}
