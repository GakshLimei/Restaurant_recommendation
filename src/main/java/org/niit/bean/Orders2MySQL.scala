package org.niit.bean

import java.sql.Timestamp

/**
 * @author: Gary Chen
 * @Created: 2023/5/11 16:21
 * @desc:
 * 用户订单信息和推荐的订单ids样例类
 */
case class Orders2MySQL(user_id: String, //用户ID
                        app_name: String, //平台ID
                        city_name: String, //城市
                        restaurant_id: String, //餐厅ID
                        food_category: String, //菜品
                        order_id: String, //订单ID
                        score: Double, //用户评分
                        order_time: String, //订单提交时间，yyyy-MM-dd HH:mm:ss字符串形式
                        ts: Timestamp //订单提交时间，时间戳形式
                       ) extends Serializable