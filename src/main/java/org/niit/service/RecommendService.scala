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

      //3.由于在ALS推荐算法只能存储纯数字东西（订单ID—32 =32）所以在后面使用模型的时候也需要将要读取的数据截取
      val id2Int = udf((user_id:String) =>{
        user_id.split("_")(1).toInt
      })

      //4.由于SparkMlib的模型只能加载sparkSQL 所以需要rdd--》dataFrame
      val ordersDF = rdd.toDF()
      val userIdDF = ordersDF.select(id2Int('user_id) as "user_id")

      //5.使用模型给用户推荐错题   推荐10道错题
      val recommendDF = model.recommendForUserSubset(userIdDF,10)
      //false 显示的时候。将省略的信息也显示出来
      recommendDF.show(false)

      //6.处理推荐结果： 取出学生id和题目id，拼接成字符串：id1,id2
      val recommendedDF = recommendDF.as[(Int,Array[(Int,Float)])].map(t =>{
        val userId:String = "用户ID_" + t._1
        val ordersId = t._2.map("订单ID" + _._2).mkString(",")
        (userId,ordersId)
      }).toDF("user_id","recommendations")

      //7.将kafka中的answer 数据和recommendDF进行合并
      val allInfoDF = ordersDF.join(recommendedDF,"user_id")

      //8.写入数据库
      allInfoDF.write
        .format("jdbc")
        .option("url", "jdbc:mysql://Node1:3306/BD2?useUnicode=true&characterEncoding=utf8")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "Niit@123")
        .option("dbtable", "edu") //写到edu表里面
        .mode(SaveMode.Append) // 追加模式，如果不存在就会自动的创建
        .save
    })

  }
}
