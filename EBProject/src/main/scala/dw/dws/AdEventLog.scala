package dw.dws

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import _root_.myutils.SourceKafka
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import _root_.myutils.AYTime

// 需求3:每隔5秒统计最近1小时内广告的点击量---增量
case class AdClick(area: String, uid: String, product_id: String, timeStamp:Long)
case class CountByProductAd(endTime: String, productId: String, count: Long)
object AdEventLog {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
     * flume-ng agent -n a1 \
     * -c /opt/servers/flume-1.9.0/conf \
     * -f /opt/servers/flume-1.9.0/conf/event-conf.properties \
     * -Dflume.root.logger=INFO,console
     */
    val kafkaConsumer: FlinkKafkaConsumer[String] = SourceKafka.getKafkaSource("eventlog")
    val eventStream: DataStream[String] = env.addSource(kafkaConsumer)
//    eventStream.print()
    /**
     * {"lagou_event":[{"name":"goods_detail_loading","json":{"entry":"2","goodsid":"0","loading_time":"92","action":"3","staytime":"10","showtype":"0"},"time":1595265099584},{"name":"notification","json":{"action":"1","type":"3"},"time":1595341087663},{"name":"ad","json":{"duration":"10","ad_action":"0","shop_id":"23","event_type":"ad","ad_type":"1","show_style":"0","product_id":"36","place":"placecampaign2_left","sort":"1"},"time":1595276738208}],"attr":{"area":"东莞","uid":"2F10092A0","app_v":"1.1.0","event_type":"common","device_id":"1FB872-9A1000","os_type":"1.1","channel":"广宣","language":"chinese","brand":"iphone-0"}}
     * area:
     * uid:
     * product_id:
     * time:
     */
    val mapEventStream: DataStream[AdClick] = eventStream.map { x =>
      val jsonObj: JSONObject = JSON.parseObject(x)
      val attr: String = jsonObj.get("attr").toString
      val attrObj: JSONObject = JSON.parseObject(attr)
      val area: String = attrObj.get("area").toString
      val uid: String = attrObj.get("uid").toString

      var productId: String = null
      var timeStamp: Long = 0
      val eventArr: JSONArray = jsonObj.getJSONArray("lagou_event")

      eventArr.forEach { evnt =>
        val eventObj = JSON.parseObject(evnt.toString)
        if (eventObj.get("name").toString == "ad") {
          val innerJson = JSON.parseObject(eventObj.get("json").toString)
          productId = innerJson.get("product_id").toString
          timeStamp = TimeUnit.MILLISECONDS.toSeconds(eventObj.get("time").toString.toLong)
        }
      }

      AdClick(area, uid, productId, timeStamp)
    }

    val filteredStream: DataStream[AdClick] = mapEventStream.filter { x => x.product_id != null }

    val result: DataStream[CountByProductAd] = filteredStream
      .assignAscendingTimestamps(x => x.timeStamp)
      .keyBy(_.product_id)
      .timeWindow(Time.seconds(20), Time.seconds(10))
      .aggregate(new AdAggFunc, new AdWindowFunc)

    result.print()

    env.execute()
  }

  class AdAggFunc extends AggregateFunction[AdClick, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: AdClick, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  class AdWindowFunc extends WindowFunction[Long, CountByProductAd, String, TimeWindow] {

    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProductAd]): Unit = {
      val acc: Long = input.iterator.next()
      out.collect(CountByProductAd(AYTime.formatTS(window.getEnd), key, acc))
    }
  }
}
