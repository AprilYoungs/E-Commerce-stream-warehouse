package ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import _root_.myutils.SourceKafka
import models.TableObject

object KafkaToHBase {
  def main(args: Array[String]): Unit = {
    // 1. consumer kafka data with FlinkKafkaConsumer
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaConsumer = SourceKafka.getKafkaSource("test")
    kafkaConsumer.setStartFromLatest()

    val sourceStream = env.addSource(kafkaConsumer)

    sourceStream.print()

    // 2. transform json data
    /**
    {"data":[{"dt":"2020-07-21","cnt":"730766","u_cnt":"585599","device_cnt":"613573","ad_action":"1","hour":"0"}],"database":"dwshow","es":1629541975000,"id":22,"isDdl":false,"mysqlType":{"dt":"varchar(10)","cnt":"bigint(20)","u_cnt":"bigint(20)","device_cnt":"bigint(20)","ad_action":"tinyint(4)","hour":"tinyint(4)"},"old":null,"pkNames":["dt","ad_action","hour"],"sql":"","sqlType":{"dt":12,"cnt":-5,"u_cnt":-5,"device_cnt":-5,"ad_action":-6,"hour":-6},"table":"ad_show","ts":1629541975548,"type":"INSERT"}
     */
    val mapped: DataStream[TableObject] = sourceStream.flatMap(x => {
      val jsonObj: JSONObject = JSON.parseObject(x)
      val typeInfo: AnyRef = jsonObj.get("type")
      val supportTypes = Array("INSERT", "UPDATE", "DELETE")
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


    // 3. sink to hbase
    mapped.addSink(new HBaseSqlObjSink())

    env.execute()
  }
}
