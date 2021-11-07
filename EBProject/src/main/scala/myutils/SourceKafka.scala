package myutils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SourceKafka {
  def getKafkaSource(topicName: String): FlinkKafkaConsumer[String] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "centos7-1:9092,centos7-2:9092,centos7-3:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset","latest")


    new FlinkKafkaConsumer[String](topicName, new SimpleStringSchema(), props)
  }
}
