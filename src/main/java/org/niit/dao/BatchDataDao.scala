package org.niit.dao

import org.apache.spark.sql.Dataset
import org.niit.bean.OrderWithRecommendations
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
  //获取takeaway表中的数据
  def getTakeawayData(): Dataset[OrderWithRecommendations] = {
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "Niit@123")
    val allInfoDS: Dataset[OrderWithRecommendations] = spark.read.jdbc(
      "jdbc:mysql://node1:3306/BD2?useUnicode=true&characterEncoding=utf8",
      "takeaway",
      props
    ).as[OrderWithRecommendations]
    //返回值
    allInfoDS
  }

}
