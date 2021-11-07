package dw.dws


import ads.MySinkToRedis
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Result, Scan, Table}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.java.tuple
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.util.Bytes
import _root_.myutils._
import models.{CityOrder, TableObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * 每隔5分钟统计最近1小时内的订单交易情况，要求显示城市、省 份、交易总金额、订单总数---增量统计
 *
 * 数据源
 * input1:dim_lagou_area(地域宽表) --- HBase
 * input2:lagou_trade_orders --- kafka
 */
object OrderStatistics {
  def main(args: Array[String]): Unit = {
    val setEnv = ExecutionEnvironment.getExecutionEnvironment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val dim_area: DataSet[tuple.Tuple2[String, String]] = setEnv.createInput(new TableInputFormat[tuple.Tuple2[String, String]] {
      //WARN: 不能自定义非没有序列化的变量
//      var conn: Connection = _

      override def configure(parameters: Configuration): Unit = {

        val conn: Connection = SourceHBase.getConnection()
        table = classOf[HTable].cast(conn.getTable(TableName.valueOf(getTableName)))
        scan = new Scan()
        scan.addFamily(Bytes.toBytes("f1"))
      }

      override def getScanner: Scan = {
        scan
      }

      override def getTableName: String = {
        "dim_lagou_area"
      }

      override def mapResultToTuple(r: Result): Tuple2[String, String] = {
        val result = r
        val rowKey = Bytes.toString(result.getRow)
        var valueString = result.listCells().asScala.map { cell =>
          val key = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          String.format("\"%s\":\"%s\"", key, value)
        }.mkString(",")
        valueString = "{" + valueString + "}"
        new Tuple2(rowKey, valueString)
      }
    })

    val area_data: DataSet[DimLagouArea] = dim_area.map{ x => JSON.parseObject(x.f1, classOf[DimLagouArea])}

    val areaList = area_data.collect().toList

    val area_map:mutable.Map[Int, DimLagouArea] = mutable.Map()
    areaList.foreach{x => area_map(x.areaid)=x}

//    println(areaList)

    val sqlStream = streamEnv.addSource(SourceKafka.getKafkaSource("test"))

    val mapped: DataStream[TableObject] = sqlStream.flatMap(x => {
      val jsonObj: JSONObject = JSON.parseObject(x)
      val typeInfo: AnyRef = jsonObj.get("type")
      val supportTypes = Array("INSERT")
      if (supportTypes.contains(typeInfo)) {
        val database: AnyRef = jsonObj.get("database")
        val table: AnyRef = jsonObj.get("table")
        var pkNames: Array[String] = Array()
        if (jsonObj.getJSONArray("pkNames") != null) {
          pkNames = jsonObj.getJSONArray("pkNames").toArray().map(_.toString)
        }
        val tableObjects: Array[TableObject] = jsonObj.getJSONArray("data").toArray.map(o => {
          TableObject(database.toString, table.toString, typeInfo.toString, pkNames, o.toString)
        })
        tableObjects
      } else {
        println("not support type: " + typeInfo)
        val nothing: Array[TableObject] = Array()
        nothing
      }
    })

    val tradeStream: DataStream[TradeOrder] = mapped.filter { t => t.tableName == "lagou_trade_orders" }
      .map{t => JSON.parseObject(t.dataInfo, classOf[TradeOrder]) }

//    tradeStream.print()
    val keyed: KeyedStream[TradeOrder, Int] = tradeStream.keyBy(_.areaId)
    val value: DataStream[(String, (Double, Int))] = keyed.flatMap{ x =>


      area_map.get(x.areaId) match {
        case Some(value) => {
          val the_area: DimLagouArea = value
          val money: Double = x.totalMoney
          val count: Int = 1
          Array((the_area.city + "-" + the_area.province, (money, count)))
        }
        case None => {
          println("area id not found --> " + x.areaId)
          val arr: Array[(String, (Double, Int))] = Array()
          arr
        }
      }
    }

    val result: DataStream[(CityOrder, Long)] = value.keyBy(_._1)
      .timeWindow(Time.seconds(60 * 10), Time.seconds(5))
      .aggregate(new MyAggFunc(), new MyWindowFunc())


//    result.print()
    result.addSink(new MySinkToRedis())

    streamEnv.execute()
  }

  class MyAggFunc() extends AggregateFunction[(String, (Double, Int)),(Double, Long),(Double, Long)] {
    override def createAccumulator(): (Double, Long) = (0, 0L)

    override def add(value: (String, (Double, Int)), accumulator: (Double, Long)): (Double, Long) = {(accumulator._1+value._2._1, accumulator._2+value._2._2)}

    override def getResult(accumulator: (Double, Long)): (Double, Long) = accumulator

    override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = {
      (a._1+b._1, a._2+b._2)
    }
  }

  class MyWindowFunc() extends WindowFunction[(Double, Long),(CityOrder, Long),String,TimeWindow] {

    override def apply(
                        key: String,
                        window: TimeWindow,
                        input: Iterable[(Double, Long)],
                        out: Collector[(CityOrder, Long)]): Unit = {
      val info: (Double, Long) = input.iterator.next()
      val totalMoney = info._1
      val count: Long = info._2
      val strings = key.split("-")
      val city = strings(0)
      val province = strings(1)
      out.collect(CityOrder(city, province, totalMoney, count), window.getEnd)
    }
  }

}
