package myutils

import org.apache.flink.configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

class HBaseWriterSink extends RichSinkFunction[Row]{
  var connection: Connection = _
  // cache tables
  var tablesMap: collection.mutable.Map[String, Table] = collection.mutable.Map()
  var tableName: String = _
  var fieldNames: Array[String] = _

  def this(tableName: String, fieldNames: Array[String])= {
    this()
    this.tableName = tableName
    this.fieldNames = fieldNames
  }

  // get from cache or create a new connection
  def getTable(tableName: String):Table = {
    tablesMap.get(tableName) match {
      case Some(table) => table
      case None => {
        val admin = connection.getAdmin.asInstanceOf[HBaseAdmin]
        // create table if not found
        // FIXME:  fix multi-thread issue
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


  override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {
    insertIntoDimArea(value)
  }

  def insertIntoDimArea(value: Row): Unit = {
    val table = getTable(tableName)
    val rowKey = value.getField(0).toString
    val put = new Put(rowKey.getBytes)

    for(i <- 0 until fieldNames.length) {
      val column = fieldNames(i)
      val data = value.getField(i).toString
      put.addColumn("f1".getBytes, column.getBytes, data.getBytes)
    }

    table.put(put)
  }

}
