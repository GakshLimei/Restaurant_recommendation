package org.niit.dao

import org.apache.spark.sql.Dataset
import org.niit.bean.{Orders, Orders2MySQL}
import org.niit.util.SparkUtil

import java.util.Properties

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:21
 * @desc:
 */
class BatchDataDao {

  private val spark = SparkUtil.takeSpark()

  import spark.implicits._
  //获取history_orders表中的数据
  def getTakeawayData(): Dataset[Orders2MySQL] = {
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "Niit@123")
    val allInfoDS: Dataset[Orders2MySQL] = spark.read.jdbc(
      "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=utf8",
      "history_orders",
      props
    ).as[Orders2MySQL]
    //返回值
    allInfoDS
  }

}
