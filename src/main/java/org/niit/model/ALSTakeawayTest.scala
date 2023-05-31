package org.niit.model

import com.google.gson.Gson
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.niit.bean.{Orders, Rating}


/**
 * @作者 YanTianCheng
 * @时间 2023/5/19 15:43
 * @文件: ALSTakeawayTest
 * @项目 org.niit.model
 */
object ALSTakeawayTest {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ALSMovieModeling")
      .config("spark.local.dir", "temp")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.加载数据并处理
    val path = "output/order_info.json"
    val ordersInfoDF: Dataset[Rating] = spark.sparkContext.textFile(path)
      .map(parseOrdersInfo)
      .toDS()
      .cache()

    val Array(trainSet, testSet) = ordersInfoDF.randomSplit(Array(0.8, 0.2)) //按照8:2划分训练集和测试集

    //TODO 2.构建ALS推荐算法模型并训练
    val als: ALS = new ALS()
      .setUserCol("user_id") //设置用户id是哪一列
      .setItemCol("restaurant_id") //设置产品id是哪一列
      .setRatingCol("rating") //设置评分列
      .setRank(50) //这个值会影响矩阵分解的性能，越大则算法运行的时间和占用的内存可能会越多。通常需要进行调参，一般可以取10-200之间的数
      .setMaxIter(10) //最大迭代次数
      .setAlpha(1.0) //迭代步长
    /*
    userCol：用户列的名字，String类型。对应于后续调用fit()操作时输入的Dataset<Row>入参时用户id所在schema中的name
    itemCol：item列的名字，String类型。对应于后续调用fit()操作时输入的Dataset<Row>入参时item id所在schema中的name
    ratingCol：rating列的名字，String类型。对应于后续调用fit()操作时输入的Dataset<Row>入参时rating值所在schema中的name

     */

    //使用训练集训练模型
    val model: ALSModel = als.fit(trainSet).setColdStartStrategy("drop")
    /*
   coldStartStrategy：String类型。
   有两个取值"nan" or "drop"。
   这个参数指示用在prediction阶段时遇到未知或者新加入的user或item时的处理策略。
   尤其是在交叉验证或者生产场景中，遇到没有在训练集中出现的user/item id时。
   "nan"表示对于未知id的prediction结果为NaN。
   "drop"表示对于transform()的入参DataFrame中出现未知ids的行，
   将会在包含prediction的返回DataFrame中被drop。默认值是"nan"
    */

    //使用测试集测试模型
    //val testResult: DataFrame = model.recommendForAllUsers(5)

    //7.对测试集进行预测
    val predictions: DataFrame = model.transform(testSet.cache())

    //计算模型误差--模型评估
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse") //均方根误差
      .setLabelCol("rating")
      .setPredictionCol("prediction") //predictionCol：String类型。做transform()操作时输出的预测值在Dataset<Row>结果的schema中的name，默认是“prediction”
    val rmse: Double = evaluator.evaluate(predictions) //均方根误差

    //显示训练集数据
    trainSet.foreach(x => println("训练集： " + x))
    //显示测试集数据
    testSet.foreach(x => println("测试集： " + x))

    //打印预测结果
    predictions.foreach(x => println("预测结果:  " + x))
    //输出误差
    println("模型误差评估：" + rmse)

    if (rmse <= 1.5) {
      val path = "output/batch_als_order_model/" + System.currentTimeMillis()
      model.save(path)


      println("模型path信息已保存")
    }


  }

  def parseOrdersInfo(json: String): Rating = {
    //1.获取用户订单信息(用户id,订单id,订单评分)
    val gson: Gson = new Gson()
    val orders: Orders = gson.fromJson(json, classOf[Orders])
    val userID: Long = orders.user_id
    val restaurantID: Long = orders.restaurant_id
    val rating: Int = orders.score

    //2.计算推荐指数:得分高的餐厅,推荐指数高
    val ratingFix: Int =
      if (rating >= 8) 3 //评分大于等于8的推荐指数为3
      else if (rating < 8 && rating <= 3) 2 //评分3-8的推荐指数2
      else 1 //其他的推荐指数为1

    //3.返回用户id，餐厅id,推荐指数
    Rating(userID, restaurantID, ratingFix)
  }

}
