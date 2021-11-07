package dw.dws

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * nc
 * 9390,1,2020-07-28 00:15:11,295
 * 5990,1,2020-07-28 00:16:12,165
 * 9390,2,2020-07-28 00:18:11,295
 * 5990,2,2020-07-28 00:18:12,165
 * 9390,3,2020-07-29 08:06:11,295
 * 5990,4,2020-07-29 12:21:12,165
 * 8457,1,2020-07-30 00:16:15,132
 * 5990,5,2020-07-30 18:13:24,165
 * 1001,1,2020-10-20 11:05:15,132
 * 1001,2,2020-10-20 11:25:15,132
 * 8458,2,2020-10-20 11:00:15,132
 */

case class OrderDetail(orderId: String, status: String, time: String, money: Double)
object OrderTimeOutCheck {
  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val data: DataStream[String] = env.socketTextStream("localhost", 7777)

    val orderDetailStream: DataStream[OrderDetail] = data.map { x =>
      val strs = x.split(",")
      OrderDetail(strs(0), strs(1), strs(2), strs(3).toDouble)
    }

    val watermarkStream: DataStream[OrderDetail] = orderDetailStream.assignTimestampsAndWatermarks(new WatermarkStrategy[OrderDetail] {
          override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[OrderDetail] = new WatermarkGenerator[OrderDetail] {

            var maxTimestamp= Long.MinValue
            var maxOutOfOrderness = 500L

            override def onEvent(event: OrderDetail, eventTimestamp: Long, output: WatermarkOutput): Unit = {
              maxTimestamp = Math.max(maxTimestamp, format.parse(event.time).getTime)
            }

            override def onPeriodicEmit(output: WatermarkOutput): Unit = {
              // 遇到第一个值之后才开始有水印
              if (maxTimestamp != Long.MinValue) {
                output.emitWatermark(new Watermark(maxTimestamp - maxOutOfOrderness))
              }
            }
          }
        }.withTimestampAssigner(new SerializableTimestampAssigner[OrderDetail] {
          override def extractTimestamp(element: OrderDetail, recordTimestamp: Long): Long = format.parse(element.time).getTime
        })
        )

//    val watermarkStream: DataStream[OrderDetail] = orderDetailStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)) {
//      override def extractTimestamp(element: OrderDetail): Long = {
//        format.parse(element.time).getTime
//      }
//    })

    val keyedStream: KeyedStream[OrderDetail, String] = watermarkStream.keyBy { x => x.orderId }

    val pattern: Pattern[OrderDetail, OrderDetail] = Pattern.begin[OrderDetail]("start")
      .where(x => x.status.equals("1"))
      .followedBy("second")
      .where(x => x.status.equals("2"))
      .within(Time.minutes(15))

    val patternStream: PatternStream[OrderDetail] = CEP.pattern(keyedStream, pattern)

    val orderTimeoutTag: OutputTag[OrderDetail] = new OutputTag[OrderDetail]("orderTimeout")
    val selectedResultStream: DataStream[OrderDetail] = patternStream.select(orderTimeoutTag, new OrderTimeoutPatternFunc, new OrderPatternFunc)


    // 获取超时的订单，有开始没结束
    selectedResultStream.getSideOutput(orderTimeoutTag).print()

    env.execute()
  }

  class OrderTimeoutPatternFunc extends PatternTimeoutFunction[OrderDetail, OrderDetail] {
    override def timeout(pattern: util.Map[String, util.List[OrderDetail]], timeoutTimestamp: Long): OrderDetail = {
      val detail: OrderDetail = pattern.get("start").iterator().next()
      detail
    }
  }

  class OrderPatternFunc extends PatternSelectFunction[OrderDetail, OrderDetail] {
    override def select(pattern: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
      val detail = pattern.get("second").iterator().next()
      detail
    }
  }
}
