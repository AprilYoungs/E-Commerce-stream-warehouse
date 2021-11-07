package dim

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import myutils.{HBaseJsonReader, HBaseReader, HBaseWriterSink}

case class AreaDetail(id:Int,name:String,pid:Int)

object AreaDetailInfo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(5000)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val hbreader = new HBaseJsonReader("lagou_area")

//    2> (810303,22.4433-114.04-00852-810303-3-中国,香港特别行政区,新界,元朗区-元朗区-810300-Yuen Long-元朗-999077)
//    2> (713621,23.5834-119.66-06-713621-3-中国,台湾,澎湖县,湖西乡-湖西乡-713600-Huxi-湖西-885)
    val sourceStream: DataStream[(String, String)] = env.addSource(hbreader)

    val dataStream: DataStream[AreaDetail] = sourceStream.map { x =>
      val fields = x._2
      JSON.parseObject(fields, classOf[AreaDetail])
//      val id = jsonObj.getInteger("id")
//      val pid = jsonObj.getInteger("pid")
//      val name = jsonObj.getString("name")
//      AreaDetail(id, name, pid)
    }

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    tableEnv.createTemporaryView("lagou_area", dataStream)
    val sqlStr =
      """
        |select a.id as areaid, a.name as aname, b.id as cid, b.name as city, c.id proid, c.name as province
        |from
        |lagou_area a
        |join lagou_area b on a.pid=b.id
        |join lagou_area c on b.pid=c.id
        |""".stripMargin

    val table: Table = tableEnv.sqlQuery(sqlStr)

    table.printSchema()

    val resultStream: DataStream[Row] = tableEnv.toRetractStream[Row](table).map{ x =>
      x._2
    }
//    resultStream.print()

    resultStream.addSink(new HBaseWriterSink("dim_lagou_area", table.getSchema.getFieldNames))

    env.execute()
  }
}
