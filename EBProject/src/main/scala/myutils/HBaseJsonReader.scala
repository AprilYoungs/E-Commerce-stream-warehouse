package myutils

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

import scala.collection.JavaConverters._

class HBaseJsonReader extends RichSourceFunction[(String, String)]{
  private var conn : Connection = _
  private var table : Table = _
  private var scan : Scan = _
  private var tableName: String = _

  def this(tableName: String) = {
    this()
    this.tableName = tableName
  }

  override def open(parameters: Configuration): Unit = {

    conn = SourceHBase.getConnection()
    table = conn.getTable(TableName.valueOf(tableName))

    scan = new Scan()

    scan.addFamily("f1".getBytes)
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {
    val rs: ResultScanner = table.getScanner(scan)
    val iterator: util.Iterator[Result] = rs.iterator()

    while (iterator.hasNext) {
      val result = iterator.next()
      val rowKey = Bytes.toString(result.getRow)
      var valueString = result.listCells().asScala.map { cell =>
        val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        String.format("\"%s\":\"%s\"", key, value)
      }.mkString(",")
      valueString = "{"+valueString+"}"
      ctx.collect((rowKey, valueString))
    }
  }

  override def cancel(): Unit = {

  }

  override def close(): Unit = {
    try {
      if (table != null ) {
        table.close()
      }
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => println(e.getMessage)
    }

  }
}
