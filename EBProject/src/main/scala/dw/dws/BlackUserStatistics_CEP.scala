package dw.dws

import java.util
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import myutils.SourceKafka
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 显示:黑名单用户ID、广告ID、点击数
 * CLICK MORE THEN 10 TIMES WITHIN 10S
 */
object BlackUserStatistics_CEP {
  case class BlackListUser(uid: String, productId: String, count: Long, endTime: String)
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

    val watermarked: DataStream[AdClick] = adClickStream
      .assignTimestampsAndWatermarks(new WatermarkStrategy[AdClick] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[AdClick] = {
          new WatermarkGenerator[AdClick] {
            var maxtimestamp = 0L
            var maxOutofOrderness = 500L

            override def onEvent(event: AdClick, eventTimestamp: Long, output: WatermarkOutput): Unit = {
              maxtimestamp = Math.max(maxtimestamp, event.timeStamp)
            }

            override def onPeriodicEmit(output: WatermarkOutput): Unit = {
              output.emitWatermark(new Watermark(maxtimestamp - maxOutofOrderness))
            }
          }
        }
      }.withTimestampAssigner(new SerializableTimestampAssigner[AdClick] {
        override def extractTimestamp(element: AdClick, recordTimestamp: Long): Long = {
          element.timeStamp
        }
      })
      )

    val keyed: KeyedStream[AdClick, (String, String)] = watermarked.keyBy(x => (x.uid, x.product_id))


    // 添加skip策略，跳过上次识别模式中的所有元素
    val pattern: Pattern[AdClick, AdClick] = Pattern
      .begin[AdClick]("begin", AfterMatchSkipStrategy.skipPastLastEvent())
      .timesOrMore(10)
      .within(Time.seconds(10))

    val patternStream: PatternStream[AdClick] = CEP.pattern(keyed, pattern)

    patternStream.select(new BlackCEPFunc).print()

    /**
     * 默认skip 策略会从上一个识别模式的第二个元素开始继续往后识别
     * 实际上只有12条记录，但是模式设定为10+，经过滑动之后就有6种组合
     * BlackListUser(2F10092A0,36,10,东莞)
     * BlackListUser(2F10092A0,36,11,东莞)
     * BlackListUser(2F10092A0,36,10,东莞)
     * BlackListUser(2F10092A0,36,12,东莞)
     * BlackListUser(2F10092A0,36,11,东莞)
     * BlackListUser(2F10092A0,36,10,东莞)
     */

    env.execute()
  }

  class BlackCEPFunc extends PatternSelectFunction[AdClick, BlackListUser] {
    override def select(pattern: util.Map[String, util.List[AdClick]]): BlackListUser = {
      val clicks = pattern.get("begin")
      val count = clicks.size()
      val click = clicks.get(1)
      BlackListUser(click.uid, click.product_id, count, click.area)
    }
  }
}
