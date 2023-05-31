package org.niit.model

import com.google.gson.Gson
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
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
    val path = "output/order_info.json"
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
      .setUserCol("user_id") //设置用户id是哪一列
      .setItemCol("restaurant_id") //设置餐厅id是哪一列
      .setRatingCol("rating") //设置评分列

    //5.使用训练集进行训练
    val model: ALSModel = als.fit(randomSplits(0)).setColdStartStrategy("drop")

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
    recommend.foreach(x => println("用户ID：" + x(0) + " ,推荐订单 " + x(1)))
    //打印预测结果
    predictions.foreach(x => println("预测结果:  " + x))
    //输出误差
    println("模型误差评估：" + rmse)

    //10.将训练好的模型保存到文件系统并将文件系统的路径存储到HBase

    //    HBaseUtil.setHTable("bigdata:student")
    if (rmse <= 1.5) {
      val path = "output/als_model/" + System.currentTimeMillis()
      model.save(path)
      //      val put: Put = new Put(Bytes.toBytes("als_model-recommended_question_id"))
      //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("path"), Bytes.toBytes(path))
      //      HBaseUtil.putData(put)
      //调用 HBaseUtil 类的 setHTable 方法，将 HBase 表名 "bigdata:student" 设置为当前对象的 HTable。
      //判断模型的 RMSE(均方根误差)是否小于等于 1.5。如果是，则执行下一步操作。
      //生成一个保存模型的路径 path,格式为 "output/als_model/" + System.currentTimeMillis()。
      //将模型保存到该路径下，并将保存后的文件名作为值，列族名为 "info",列名为 "path",列值为字节数组 path。
      //调用 HBaseUtil 类的 putData 方法，将保存好的模型数据 put 存储到 HBase 表中。

      //      println("模型path信息已保存到HBase")

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
    //1.获取用户订单信息(用户id,订单id,订单评分)
    val gson: Gson = new Gson()
    val orders: Orders = gson.fromJson(json, classOf[Orders])
    val userID: Long = orders.user_id
    val restaurantID: Long = orders.restaurant_id
    val rating: Int = orders.score

    //2.计算推荐指数:得分高的订单,推荐指数高
    val ratingFix: Int =
      if (rating >= 8) 3 //评分大于等于8的推荐指数为3
      else if (rating < 8 && rating <= 3) 2 //评分3-8的推荐指数2
      else 1 //其他的推荐指数为1

    //3.返回用户id，问题id,推荐指数
    Rating(userID, restaurantID, ratingFix)
  }

}
