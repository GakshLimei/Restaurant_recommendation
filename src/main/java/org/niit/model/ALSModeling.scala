package org.niit.model

import com.google.gson.Gson
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.niit.bean.{Orders, Rating}
import org.niit.util.HBaseUtil

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
      .toDS()
      .cache()

    //3.划分数据集Array(80%训练集, 20%测试集)
    val randomSplits: Array[Dataset[Rating]] = ordersInfoDF.randomSplit(Array(0.8, 0.2), 11L)

    //4.构建ALS模型
    val als: ALS = new ALS()
      .setRank(20)
      .setMaxIter(15)
      .setRegParam(0.09)
      .setUserCol("user_id")
      .setItemCol("orders_id")
      .setRatingCol("rating")

    //5.使用训练集进行训练
    val model:ALSModel = als.fit(randomSplits(0)).setColdStartStrategy("drop")

    //6.获得推荐
    val recommend: DataFrame = model.recommendForAllUsers(20)

    //7.对测试集进行预测
    val predictions: DataFrame = model.transform(randomSplits(1).cache())

    //8.使用RMSE(均方根误差)评估模型误差
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse") //均方根误差
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse: Double = evaluator.evaluate(predictions) //均方根误差

    //9.输出结果
    //显示训练集数据
    randomSplits(0).foreach(x => println("训练集： " + x))
    //显示测试集数据
    randomSplits(1).foreach(x => println("测试集： " + x))
    //推荐结果
    recommend.foreach(x => println("学生ID：" + x(0) + " ,推荐题目 " + x(1)))
    //打印预测结果
    predictions.foreach(x => println("预测结果:  " + x))
    //输出误差
    println("模型误差评估：" + rmse)

    //10.将训练好的模型保存到文件系统并将文件系统的路径存储到HBase

    HBaseUtil.setHTable("bigdata:student")
    if (rmse <= 1.5) {
      val path = "output/als_model/" + System.currentTimeMillis()
      model.save(path)
      val put: Put = new Put(Bytes.toBytes("als_model-recommended_question_id"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("path"), Bytes.toBytes(path))
      HBaseUtil.putData(put)

      println("模型path信息已保存到HBase")

  }
    //11.释放缓存/关闭资源
    ordersInfoDF.unpersist()
    randomSplits(0).unpersist()
    randomSplits(1).unpersist()
    //RedisUtil.pool.returnResource(jedis)
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
    val ratingFix: Int =
      if (rating >= 3) 3
      else if (rating > 3 && rating <= 8) 2
      else 1

      //3.返回用户id，问题id,推荐指数
      Rating(userID, ordersID, ratingFix)
  }

}
