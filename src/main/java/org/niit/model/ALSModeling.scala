package org.niit.model

import com.google.gson.Gson
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.sql.{Dataset, SparkSession}
import org.niit.bean.{Orders, Rating}

/**
 * @作者 YanTianCheng
 * @时间 2023/5/4 20:41
 * @文件: ALSModeling
 * @项目 org.niit.model
 */
object ALSModeling {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ALSModeling")
      .config("spark.local.dir", "temp")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //2.加载数据并转换为：Dataset[Rating(用户id,订单id,推荐指数）]
    val path = "output/orders_info.json"
    val ordersInfoDF: Dataset[Rating] = spark.sparkContext.textFile(path)
      .map(parseOrdersInfo)
      .toDs()
      .cache()

    //3.划分数据集Array(80%训练集, 20%测试集)
    val randomSpalits: Array[Dataset[Rating]] = ordersInfoDF.randomSplit(Array(0.8, 0.2), 11)

    //4.构建ALS模型
    val als: ALS = new ALS()
      .setRank(20)
      .setMaxIter(15)
      .setRegParam(0.09)


  }

  /**
   * 将用户订单的详细信息转为Rating(用户id,订单id,推荐指数)
   */
  def parseOrdersInfo(json: String): Rating = {
    //1.获取用户订单信息(用户id,订单id,推荐指数)
    val gson: Gson = new Gson()
    val orders: Orders = gson.fromJson(json, classOf[Orders])
    val userID: Long = orders.user_id.split("_")(1).toLong
    val ordersID: Long = orders.order_id.split("_")(1).toLong
    val rating: Int = orders.score

    //2.计算推荐指数:得分低的题目,推荐指数高
    val ratingFix: Int = {
      if (rating >= 3) 3
      else if (rating > 3 && rating <= 8) 2
      else 1

      //3.返回用户id，问题id,推荐指数
      Rating(userID, ordersID, ratingFix)
    }

  }

}
