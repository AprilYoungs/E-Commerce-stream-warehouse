package dw.dws

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import myutils.SourceKafka
import _root_.myutils.AYTime
import org.apache.flink.streaming.api.TimeCharacteristic

case class BlackListUser(uid: String, productId: String, count: Long, endTime: String)

/**
 * 显示:黑名单用户ID、广告ID、点击数
 * CLICK MORE THEN 10 TIMES WITHIN 10S
 */
object BlackUserStatistics {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConsumer: FlinkKafkaConsumer[String] = SourceKafka.getKafkaSource("eventlog")
    val data: DataStream[String] = env.addSource(kafkaConsumer)

    val adClickStream = data.map { x =>
      val jsonObj: JSONObject = JSON.parseObject(x)
      val attr: String = jsonObj.get("attr").toString
      val attrObj: JSONObject = JSON.parseObject(attr)
      val area: String = attrObj.get("area").toString
      val uid: String = attrObj.get("uid").toString
      var productId: String = null
      var timestamp: Long = 0L

      val events: JSONArray = jsonObj.getJSONArray("lagou_event")
      events.forEach { xx =>
        val nObject = JSON.parseObject(xx.toString)
        if (nObject.get("name").equals("ad")) {
          val adObj = nObject.getJSONObject("json")
          productId = adObj.getString("product_id")
          timestamp = TimeUnit.MILLISECONDS.toSeconds(nObject.getLong("time"))
        }
      }

      AdClick(area, uid, productId, timestamp)
    }

    val result: DataStream[BlackListUser] = adClickStream
      .assignAscendingTimestamps{x => x.timeStamp}
      .keyBy(x => (x.uid, x.product_id))
      .timeWindow(Time.seconds(10)
      )
      .aggregate(new BlackAggFunc, new BlackWindowFunc)

    result.filter{x => x.count>=10}.print()

    env.execute()
  }

  class BlackAggFunc extends AggregateFunction[AdClick, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClick, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  class BlackWindowFunc extends WindowFunction[Long,BlackListUser,(String, String), TimeWindow] {
    override def apply(key: (String, String), window: TimeWindow, input: Iterable[Long], out: Collector[BlackListUser]): Unit = {

      out.collect(BlackListUser(key._1, key._2, input.iterator.next(), AYTime.formatTS(window.getEnd)))

    }
  }
}
