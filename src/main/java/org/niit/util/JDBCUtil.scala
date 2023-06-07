package org.niit.util

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

object JDBCUtil {
  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://node1:3306/Takeaway?useUnicode=true&characterEncoding=UTF-8")
    properties.setProperty("username", "root")
    properties.setProperty("password", "Niit@123")
    properties.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //执行SQL语句,单条数据插入
  def executeUpdate(connection: Connection, sql: String): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)


      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  // 创建表如果表不存在
  def createTableIfNotExists(connection: Connection, tableName: String, tableSchema: String): Unit = {
    val sql = s"CREATE TABLE IF NOT EXISTS $tableName ($tableSchema)"
    executeUpdate(connection, sql)
  }

  //判断一条数据是否存在
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  //获取 MySQL 的一条数据
  def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]):
  Long = {
    var result: Long = 0L
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

}
