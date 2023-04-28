package org.niit.bean

import java.sql.Timestamp

/**
 * 学生答题信息样例类
 *
 * {
 * "student_id": "学生ID_41"
 * , "textbook_id": "教材ID_1"
 * , "grade_id": "年级ID_2"
 * , "subject_id": "科目ID_1_数学"
 * , "chapter_id": "章节ID_chapter_1"
 * , "question_id": "题目ID_181"
 * , "score": 2
 * , "answer_time": "2020-11-20 17:05:28"
 * , "ts": "Nov 20, 2020 5:05:28 PM"
 * }
 *
 * 转换过程（映射） 键名相同 属性名相同，可以将值赋给属性
 * 约定 > 编码
 */

//all:用户，平台（例如美团），城市，餐厅，菜品类别（烧烤，火锅......），
//订单ID，用户评分，提交时间，时间戳
case class Orders(user_id: String, //用户ID
                  app_id: String, //平台ID
                  city_id: String, //城市ID
                  restaurant_id: String, //餐厅ID
                  food_category_id: String, //菜品ID
                  order_id: String, //订单ID
                  score: Int, //用户评分
                  order_time: String, //订单提交时间，yyyy-MM-dd HH:mm:ss字符串形式
                  ts: Timestamp //答题提交时间，时间戳形式
                 ) extends Serializable
case class Answer(student_id: String, //学生ID
                  textbook_id: String, //教材ID
                  grade_id: String, //年级ID
                  subject_id: String, //科目ID
                  chapter_id: String, //章节ID
                  question_id: String, //题目ID
                  score: Int, //题目得分，0~10分
                  answer_time: String, //答题提交时间，yyyy-MM-dd HH:mm:ss字符串形式
                  ts: Timestamp //答题提交时间，时间戳形式
                 ) extends Serializable
