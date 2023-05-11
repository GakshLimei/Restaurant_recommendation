package org.niit.bean

/**
 * @作者 YanTianCheng
 * @时间 2023/5/4 20:41
 * @文件: Rating
 * @项目 org.niit.bean
 */
case class Rating(
                 user_id:Long,    //用户id
                 restaurant_id:Long,  //餐厅id
                 rating:Float  //推荐指数
                 )extends Serializable
