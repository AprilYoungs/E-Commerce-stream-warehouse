package dw.dws

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import myutils.HBaseJsonReader
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 *  需求1 : 查询城市、省份、订单总额、订单总数----全量查询
 * 1. dim_lagou_area dim
 * 2. lagou_trade_orders inc
 * join to stream
 */

case class DimLagouArea(areaid:Int,aname:String,cid:Int,city:String,proid:Int,province:String) extends Serializable
case class TradeOrder(areaId:Int,createTime:String,dataFlag:String,isPay:Boolean,isRefund:Boolean,modifiedTime:String,orderId:Int,orderNo:String,payMethod:String,payTime:String,productMoney:Double,status:String,totalMoney:Double,tradeSrc:String,tradeType:String,userId:Int) extends Serializable

object TotalCityOrder {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val dimStream: DataStream[(String, String)] = env.addSource(new HBaseJsonReader("dim_lagou_area"))
    /**
     *  1> (0043d482-f9a5-4d8f-9e43-b43a6ad2ef9e,{"aname":"泰顺县","areaid":"330329","cid":"330300","city":"温州市","proid":"330000","province":"浙江省"})
     */

    val dimData: DataStream[DimLagouArea] = dimStream.map { x =>
      JSON.parseObject(x._2, classOf[DimLagouArea])
    }

    val orderStream: DataStream[(String, String)] = env.addSource(new HBaseJsonReader("lagou_trade_orders"))
//    orderStream.print()
    /**
     * 6> (1,{"areaId":"370203","createTime":"2020-06-28 18:14:01","dataFlag":"2","isPay":"0","isRefund":"1","modifiedTime":"2020-10-21 22:54:31","orderId":"1","orderNo":"23a0b124546","payMethod":"2","payTime":"2020-06-28 18:14:01","productMoney":"0.12","status":"2","totalMoney":"10468.0","tradeSrc":"0","tradeType":"0","userId":"98"})
     */
    val tradeData: DataStream[TradeOrder] = orderStream.map { x =>
      JSON.parseObject(x._2, classOf[TradeOrder])
    }
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.createTemporaryView("dim_lagou_area", dimData)
    tableEnv.createTemporaryView("lagou_trade_orders", tradeData)

    val citySqlStr =
      """
        select area.city as city, area.province as province, sum(trade.totalMoney) as totalMoney, count(1) as `count`
        |from dim_lagou_area area
        |join lagou_trade_orders trade
        |on area.areaid = trade.areaId
        |group by area.city, area.province
        |""".stripMargin

    val cityTable: Table = tableEnv.sqlQuery(citySqlStr)
    cityTable.printSchema()

    val cityResult: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](cityTable)

    // 只要增量和变更的数据
    cityResult.filter{x => x._1}.print()

    /**
     * 5> (true,北京市,北京,101325.0,5)
     * 8> (true,青岛市,山东省,490558.36,22)
     */

    env.execute()
  }
}
