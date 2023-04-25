package org.niit.util

import java.util
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
 * @作者 YanTianCheng
 * @时间 2023/4/24 10:51
 * @文件: HBaseUtil
 * @项目 org.niit.util
 */
object HBaseUtil {

  private val conn = init()

  private var hTable:Table = _;

  private def init(): Connection ={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","node1")
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }

  //1. 声明名为“init”的方法，返回类型为“连接”。
  //2. HBaseConfiguration 对象是使用 HBaseConfiguration.create（） 方法创建的。
  //3. HBaseConfiguration 对象配置为将 ZooKeeper 仲裁设置为“node1”。
  //4. 使用 ConnectionFactory.createConnection（） 方法创建一个连接，传入 HBaseConfiguration 对象。
  //5. 从方法返回连接。

  def getConnection:Connection={
    conn
  }
  def setHTable(tableName:String):Table={
    val hbaseTableName: TableName = TableName.valueOf(tableName)
    val hbaseTable = conn.getTable(hbaseTableName)
    hTable = hbaseTable
    hbaseTable
  }

  def putData(put: Put): Unit ={
    hTable.put(put)
    hTable.close()
    conn.close()
  }

  def putDatas(puts:java.util.List[Put]): Unit ={

    hTable.put(puts)
    hTable.close()
    conn.close()
  }

  def getData(get:Get): List[String] ={
    var value = List[String]()
    val result = hTable.get(get)
    val cells: util.List[Cell] = result.listCells()
    cells.forEach(c=>{
      var res = Bytes.toString(c.getValueArray,c.getValueOffset,c.getValueLength)
      value = value:+res
    })
    value
  }

  def main(args: Array[String]): Unit = {
    HBaseUtil.setHTable("bigdata:student")
    //val puts = new util.ArrayList[Put]()
    //    val put: Put = new Put(Bytes.toBytes("yyy1111"))
    //    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"))
    //    //puts.add(put)
    //    HBaseUtil.putData(put)
    val get = new Get(Bytes.toBytes("yyy1111"))
    val value = HBaseUtil.getData(get)
    println(value)

  }

}
