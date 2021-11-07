package dw.joins

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * 有订单流OrderEvent
 * 支付事件流PayEvent
 * 双流join，实现一个订单正常支付输出，超时未支付输出的demo
 */
object CoProcessFunc {
  case class OrderEvent(orderId:String,eventType:String,eventTime:Long)
  case class PayEvent(orderId:String,eventType:String,eventTime:Long)

  val unmatchedOrders = new OutputTag[String]("unmatched-orders")
  val unmatchedPays = new OutputTag[String]("unmatched-pays")
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderStream: KeyedStream[OrderEvent, String] = env.fromElements(
      OrderEvent("order_1", "pay", 2000L),
      OrderEvent("order_2", "pay", 5000L),
      OrderEvent("order_3", "pay", 6000L)
    )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    val payStream: KeyedStream[PayEvent, String] = env.fromElements(
      PayEvent("order_1", "weixin", 7000L),
      PayEvent("order_2", "weixin", 8000L),
      PayEvent("order_4", "weixin", 9000L)
    )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)


    val processed: DataStream[String] = orderStream.connect(payStream).process(new MyConFunc)
    processed.print()

    processed.getSideOutput(unmatchedOrders).print()
    processed.getSideOutput(unmatchedPays).print()

    env.execute()
  }

  /**
   * 就像两只手接住两排连续的小球，但是球太烫了只能握住5s，
   * 接到第一个球之后5s内又接到第二个球就match
   * 不然5s到了就抛出手里的球，并说明是左手的球，还是右手的球
   */
  class MyConFunc extends KeyedCoProcessFunction[String, OrderEvent, PayEvent, String] {
    lazy private val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("orderState", Types.of[OrderEvent]))
    lazy private val payState: ValueState[PayEvent] = getRuntimeContext.getState(new ValueStateDescriptor[PayEvent]("payState", Types.of[PayEvent]))

    override def processElement1(value: OrderEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, PayEvent, String]#Context, out: Collector[String]): Unit = {
      val pay = payState.value()
      if (pay != null) {
        payState.clear()
        out.collect("order id:" + pay.orderId + " matched")
      } else {
        orderState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime+5000)
      }
    }

    override def processElement2(value: PayEvent, ctx: KeyedCoProcessFunction[String, OrderEvent, PayEvent, String]#Context, out: Collector[String]): Unit = {
      val order = orderState.value()
      if (order != null) {
        orderState.clear()
        out.collect("order id: " + order.orderId + " matched")
      } else {
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime + 5000)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, PayEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (orderState.value() != null) {
        ctx.output(unmatchedOrders, s"order id: ${orderState.value().orderId} fail to match")
        orderState.clear()
      }

      if (payState.value() != null) {
        ctx.output(unmatchedPays , s"order id: ${payState.value().orderId} fail to match")
        payState.clear()
      }
    }
  }
}
