package org.niit.bean

/**
 * @作者 YanTianCheng
 * @时间 2023/5/4 20:41
 * @文件: Rating
 * @项目 org.niit.bean
 */
case class Rating(
                 user_id:Long,    //用户id
                 orders_id:Long,  //订单id
                 rating:Float  //推荐指数
                 )extends Serializable
