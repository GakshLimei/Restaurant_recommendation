package org.niit.bean

import java.sql.Timestamp

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:21
 * @desc:
 * 用户订单信息和推荐的订单ids样例类
 */
case class OrderWithRecommendations(
                                     user_id: String, //用户ID
                                     app_id: String, //平台ID
                                     city_id: String, //城市ID
                                     restaurant_id: String, //餐厅ID
                                     food_category_id: String, //菜品ID
                                     order_id: String, //订单ID
                                     score: Int, //用户评分
                                     order_time: String, //订单提交时间，yyyy-MM-dd HH:mm:ss字符串形式
                                     ts: Timestamp //订单提交时间，时间戳形式
                                   ) extends Serializable