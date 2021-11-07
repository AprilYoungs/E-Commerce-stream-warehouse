package dw.dws

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import _root_.myutils._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class ChannelData(channel: String, uid: String)
case class ChannelStat(channel: String, userCount: Long)

/**
 * 需求5:实时统计各渠道来源用户数量
 * 统计不同渠道用户数量，去重
 */
object ChannelStatistics {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val data: DataStream[String] = env.addSource(SourceKafka.getKafkaSource("eventlog"))

    val channelStream: DataStream[ChannelData] = data.map { x =>
      val jsonObj: JSONObject = JSON.parseObject(x)
      val attr: String = jsonObj.get("attr").toString
      val attrObj: JSONObject = JSON.parseObject(attr)
      val channel: String = attrObj.get("channel").toString
      val uid: String = attrObj.get("uid").toString

      ChannelData(channel, uid)
    }

    val result = channelStream
      .keyBy { x => x.channel }
      .timeWindow(Time.seconds(10))
      .aggregate(new ChannelAggFunc, new ChannelWindowFunc)

    val finResult: DataStream[String] = result.process(new MyProcessFunc)

    finResult.print()

    env.execute()
  }

  class ChannelAggFunc extends AggregateFunction[ChannelData, Set[String], Long] {
    override def createAccumulator(): Set[String] = Set()

    override def add(value: ChannelData, accumulator: Set[String]): Set[String] = accumulator.+(value.uid)

    override def getResult(accumulator: Set[String]): Long = accumulator.size

    override def merge(a: Set[String], b: Set[String]): Set[String] = a ++ b
  }

  class ChannelWindowFunc extends WindowFunction[Long, ChannelStat, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[ChannelStat]): Unit = {
      out.collect(ChannelStat(key, input.iterator.next()))
    }
  }

  class MyProcessFunc extends ProcessFunction[ChannelStat, String] {
    override def processElement(value: ChannelStat, ctx: ProcessFunction[ChannelStat, String]#Context, out: Collector[String]): Unit = {
      val desc = s"channel: ${value.channel}, user count: ${value.userCount}"
//      println(desc)
      out.collect(desc)
    }
  }
}


