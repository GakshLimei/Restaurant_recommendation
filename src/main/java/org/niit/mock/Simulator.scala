package org.niit.mock

import com.google.gson.Gson
import org.niit.bean.Orders

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * @author: Gary Chen
 * @Created: 2023/4/28 18:38
 * @desc: 模拟数据生成器
 */

/**
 * 在线教育学生学习数据模拟程序
 */
object Simulator {
  //模拟数据
  //用户ID
  val arr1 = ArrayBuffer[String]()
  for (i <- 1 to 50) {
    arr1 += "用户ID_" + i
  }
  //平台ID
  val arr2 = Array("平台ID_1", "平台ID_2")
  //城市ID
  val arr3 = Array("城市ID_1", "城市ID_2", "城市ID_3", "城市ID_4", "城市ID_5", "城市ID_6")
  //餐厅ID
  val arr4 = Array("餐厅ID_1_", "餐厅ID_2_", "餐厅ID_3_","餐厅ID_4_")
  //菜品ID
  val arr5 = Array("菜品ID_category_1", "菜品ID_category_2", "菜品ID_category_3")

  //订单ID与平台、城市、餐厅、菜品的对应关系,
  val questionMap = collection.mutable.HashMap[String, ArrayBuffer[String]]()
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  for (textbookID <- arr2; gradeID <- arr3; subjectID <- arr4; chapterID <- arr5) {
    val key = new StringBuilder()
      .append(textbookID).append("^")
      .append(gradeID).append("^")
      .append(subjectID).append("^")
      .append(chapterID)

    val orderArr = ArrayBuffer[String]()
    for (i <- 1 to 20) {
      orderArr += "订单ID_" + questionID
      questionID += 1
    }
    questionMap.put(key.toString(), orderArr)
  }
  var orderID = 1

  //测试模拟数据
  def main(args: Array[String]): Unit = {
    val printWriter = new PrintWriter(new File("output/order_info.json"))
    val gson = new Gson()
    for (i <- 1 to 2000) {
      println(s"第{$i}条")
      val jsonString = gson.toJson(genQuestion())
      println(jsonString)
      //{"student_id":"学生ID_44","textbook_id":"教材ID_1","grade_id":"年级ID_4","subject_id":"科目ID_3_英语","chapter_id":"章节ID_chapter_3","question_id":"题目ID_701","score":10,"answer_time":"2020-01-11 17:20:42","ts":"Jan 11, 2020 5:20:42 PM"}

      printWriter.write(jsonString + "\n")
      //Thread.sleep(200)
    }
    printWriter.flush()
    printWriter.close()
  }

  def genQuestion() = {
    //随机平台ID
    val appIDRandom = arr2(Random.nextInt(arr2.length))
    //随机年级ID
    val gradeIDRandom = arr3(Random.nextInt(arr3.length))
    //随机科目ID
    val subjectIDRandom = arr4(Random.nextInt(arr4.length))
    //随机章节ID
    val chapterIDRandom = arr5(Random.nextInt(arr5.length))

    val key = new StringBuilder()
      .append(appIDRandom).append("^")
      .append(gradeIDRandom).append("^")
      .append(subjectIDRandom).append("^")
      .append(chapterIDRandom)

    //取出题目
    val questionArr = questionMap(key.toString())
    //随机题目ID
    val questionIDRandom = questionArr(Random.nextInt(questionArr.length))
    //随机题目扣分
    val deductScoreRandom = Random.nextInt(11) + 1
    //随机学生ID
    val studentID = arr1(Random.nextInt(arr1.length))
    //答题时间
    val ts = System.currentTimeMillis()
    val timestamp = new Timestamp(ts)
    val answerTime = sdf.format(new Date(ts))

    Orders(studentID, appIDRandom, gradeIDRandom, subjectIDRandom, chapterIDRandom, questionIDRandom, deductScoreRandom, answerTime, timestamp)
  }
}