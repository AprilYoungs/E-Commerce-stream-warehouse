package dw.joins

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 根据时间窗口去join，跟intervalJoin相比，更加以窗口window为中心
 */
object WindowJoin {
  case class UserClickLog(userId: String, eventTime: String, eventType: String, pageId:
  String)
  case class UserBrowseLog(userId: String, eventTime: String, productId: String, productPrice: String)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val input1Stream: DataStream[(Int, Long)] = env.fromElements((1, 1999L), (1,
      2000L),(1, 2001L)).assignAscendingTimestamps(_._2)
    val input2Stream: DataStream[(Int, Long)] = env.fromElements((1, 1000L),(1, 1001L), (1, 1002L), (1, 1500L),(1,
      3999L),(1, 4000L)).assignAscendingTimestamps(_._2)

    input1Stream.join(input2Stream)
        .where(k => k._1)   //left key
        .equalTo(k=>k._1)   //right key
        .window(TumblingEventTimeWindows.of(Time.seconds(2)))  //window
        .apply{(e1, e2) => e1 + "...." + e2}  //
        .print()

    /**
     * (1,1999)....(1,1000)
     * (1,1999)....(1,1001)
     * (1,1999)....(1,1002)
     * (1,1999)....(1,1500)
     * (1,2000)....(1,3999)
     * (1,2001)....(1,3999)
     */
    // .window(TumblingEventTimeWindows.of(Time.seconds(2)))
    //  滚动窗口，默认开始偏移量是0，所以从0～1999是一个窗口，2000～3999 是一个窗口
    env.execute()
  }
}
