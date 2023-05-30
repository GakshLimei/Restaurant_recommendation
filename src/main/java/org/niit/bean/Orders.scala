package org.niit.bean

import java.sql.Timestamp

/**
 * @author: Gary Chen
 * @Created: 2023/4/29 18:43
 * @desc: 订单信息样例类
 * @param user_id
 * @param app_id
 * @param city_id
 * @param restaurant_id
 * @param food_category_id
 * @param order_id
 * @param score
 * @param order_time
 * @param ts
 */
//all:用户，平台（例如美团），城市，餐厅，菜品类别（烧烤，火锅......），
//订单ID，用户评分，提交时间，时间戳
case class Orders(user_id: Long, //用户ID
                  app_name: String, //平台ID
                  city_name: String, //城市ID
                  restaurant_id: Long, //餐厅ID
                  food_category: String, //菜品ID
                  order_id: Long, //订单ID
                  score: Int, //用户评分
                  order_time: String, //订单提交时间，yyyy-MM-dd HH:mm:ss字符串形式
                  ts: Timestamp //答题提交时间，时间戳形式
                 ) extends Serializable

