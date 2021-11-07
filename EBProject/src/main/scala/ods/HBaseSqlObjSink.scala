package ods


import java.util.UUID

import com.alibaba.fastjson.JSON
import models.TableObject
import myutils.SourceHBase
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf.Configuration
import org.apache.flink.configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, HBaseAdmin, Put, Table}

/**
 * 解析通过canal传过来的json对象，并下沉到hbase
 */
class HBaseSqlObjSink extends RichSinkFunction[TableObject]{
  var connection: Connection = _
  // cache tables
  var tablesMap: collection.mutable.Map[String, Table] = collection.mutable.Map()

  // get from cache or create a new connection
  def getTable(tableName: String):Table = {
    tablesMap.get(tableName) match {
      case Some(table) => table
      case None => {
        val admin = connection.getAdmin.asInstanceOf[HBaseAdmin]
        // create table if not found
        if (!admin.tableExists(tableName)) {
          val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
          tableDesc.addFamily(new HColumnDescriptor("f1"))
          admin.createTable(tableDesc)
        }
        connection.getTable(TableName.valueOf(tableName))
      }
    }
  }


  override def open(parameters: configuration.Configuration): Unit = {

    connection = SourceHBase.getConnection()

  }

  override def close(): Unit = {
    // release all tables
    tablesMap.values.foreach{table => table.close()}

    if (connection != null) {
      connection.close()
    }
  }


  override def invoke(value: TableObject, context: SinkFunction.Context[_]): Unit = {
    if (value.database.equalsIgnoreCase("dwshow")) {

      if (value.typeInfo.equalsIgnoreCase("insert")) {
        putOneRecord(value)
      } else if (value.typeInfo.equalsIgnoreCase("update")) {
        putOneRecord(value)
      } else if (value.typeInfo.equalsIgnoreCase("delete")) {
        deleteOneRecord(value)
      }
    }
  }

  /**
   * dynamic put value to table
   * @param tableObj
   */
  def putOneRecord(tableObj: TableObject): Unit = {

    val tableName = tableObj.tableName
    val hbTable = getTable(tableName)
    val jsonData = JSON.parseObject(tableObj.dataInfo)
    // handle none pkNames
    var primaryKey = UUID.randomUUID().toString
    if (!tableObj.pkNames.isEmpty) {
      primaryKey = tableObj.pkNames.map(jsonData.get(_).toString).mkString("_")
    }
    println("table:"+hbTable)
    println("primaryKey:"+primaryKey)
    val put = new Put(primaryKey.getBytes)
    // loop all values
    jsonData.entrySet().forEach{kv =>
      put.addColumn("f1".getBytes, kv.getKey.getBytes, kv.getValue.toString.getBytes)
    }
    hbTable.put(put)
  }
  def deleteOneRecord(tableObj: TableObject): Unit = {
    val tableName = tableObj.tableName
    val hbTable = getTable(tableName)
    val jsonData = JSON.parseObject(tableObj.dataInfo)
    val primaryKey = tableObj.pkNames.map(jsonData.get(_).toString).mkString("_")
    println("------------------delete")
    println("table:"+hbTable)
    println("primaryKey:"+primaryKey)
    val delete = new Delete(primaryKey.getBytes)
    hbTable.delete(delete)
  }
}
