package org.niit.service

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.dstream.DStream
import org.niit.util.{HBaseUtil, SparkUtil}
import org.niit.bean.Orders

/**
 * @作者 YanTianCheng
 * @时间 2023/5/3 20:55
 * @文件: RecommendService
 * @项目 org.niit.service
 * 推荐系统
 */
class RecommendService {
  val spark = SparkUtil.takeSpark()
  import spark.implicits._
  import org.apache.spark.sql.functions._

  def dataAnalysis(orders: DStream[Orders]): Unit = {
    orders.foreachRDD(rdd =>{
      //1.获取训练好的模型路径
      //如果是存在HBase中
//      HBaseUtil.setHTable("bigdata:takeaway")
//      val value = HBaseUtil.getData(new Get(Bytes.toBytes("als_model-recommended_orders_id")))
//      val path = value(0)
      val path = "D:\\spark\\usr\\local\\spark\\Takeaway\\Restaurant_recommendation\\output\\als_model\\1683291533402"

      //2.加载模型
      val model = ALSModel.load(path)

      //3.由于在ALS推荐算法只能存储纯数字东西（用户ID—32 =32）所以在后面使用模型的时候也需要将要读取的数据截取
      val id2Int = udf((user_id:String) =>{
        user_id.split("_")(1).toInt    //根据 _ 分割数组，分割后取第二个元素为id，并调用toInt将其转换为整数类型
      })

      //4.由于SparkMlib的模型只能加载sparkSQL 所以需要rdd--》dataFrame
      val ordersDF = rdd.toDF()   //将RDD转换为DataFrame，将其命名为 ordersDF
      val userIdDF = ordersDF.select(id2Int('user_id) as "user_id")    //调用 select 方法并传入 id2Int('user_id) 表达式，以将 user_id 转换为整数类型,使用 as 关键字给新生成的列起个别名 user_id

      //5.使用模型给用户推荐餐厅  推荐10个高质量餐厅
      //调用协同过滤模型 model 的 recommendForUserSubset() 方法。
      // 该方法需要传入两个参数：一个包含用户ID的DataFrame，和一个整数类型的参数 numItems，表示要返回的每个用户的推荐项目数量。
      val recommendDF = model.recommendForUserSubset(userIdDF,3)
      //false 显示的时候。将省略的信息也显示出来
      recommendDF.show(false)  //执行 show() 方法,参数设置为 false，以便查看所有列的完整信息。

      //6.处理推荐结果： 取出学生id和餐厅id，拼接成字符串：id1,id2
      val recommendedDF = recommendDF.as[(Int,Array[(Int,Float)])].map(t =>{
        val userId:String = "用户ID_" + t._1
        val restaurantId = t._2.map("餐厅ID" + _._2).mkString(",")
        (userId,restaurantId)
      }).toDF("user_id","recommendations")
      //将 DataFrame 转换为 RDD
      //将 RDD 转换回 DataFrame，并将 user_id 和 recommendations 列分别作为 DataFrame 的 user_id 和 recommendations 列
      //将 RDD 转换为 DataFrame，并将 user_id 和 recommendations 列分别作为 DataFrame 的 user_id 和 recommendations 列

      //7.将kafka中的orders 数据和recommendDF进行合并
      val allInfoDF = ordersDF.join(recommendedDF,"user_id")

      //8.写入数据库
      allInfoDF.write
        .format("jdbc")
        .option("url", "jdbc:mysql://Node1:3306/BD2?useUnicode=true&characterEncoding=utf8")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "Niit@123")
        .option("dbtable", "takeaway") //写到edu表里面
        .mode(SaveMode.Append) // 追加模式，如果不存在就会自动的创建
        .save
    })

  }
}
