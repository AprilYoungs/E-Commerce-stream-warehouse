package ads

import java.util

import models.CityOrder
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.Jedis

class MySinkToRedis extends RichSinkFunction[Tuple2[CityOrder, Long]]{
  var jedis: Jedis = null

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("centos7-2", 6379, 6000)
//    jedis.auth("psw")
    jedis.select(0)
  }

  override def invoke(value: (CityOrder, Long), context: SinkFunction.Context[_]): Unit = {
    if (!jedis.isConnected) {
      jedis.connect()
    }

    val map = new util.HashMap[String, String]()
    map.put("totalMoney", value._1.totalMoney.toString)
    map.put("count", value._1.count.toString)
    map.put("time", value._2.toString)

    jedis.hset(value._1.province+value._1.city, map)
  }

  override def close(): Unit = {jedis.close()}
}
