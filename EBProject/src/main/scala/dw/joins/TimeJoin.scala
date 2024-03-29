package dw.joins

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 根据双流的的变量之间时间间隔去join，和windowJoin比较，更加以数据为中心
 */
object TimeJoin {
  case class UserClickLog(userId: String, eventTime: String, eventType: String, pageId:
  String)
  case class UserBrowseLog(userId: String, eventTime: String, productId: String, productPrice: String)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val clickStream = env.fromElements(
      UserClickLog("user_1", "1500", "click", "page_1"),
      UserClickLog("user_1", "2000", "click", "page_1")
    )
      .assignAscendingTimestamps(_.eventTime.toLong*1000)
      .keyBy(_.userId)

    val browseStream = env.fromElements(
      UserBrowseLog("user_1", "1000", "product_1", "10"),
      UserBrowseLog("user_1", "1500", "product_1", "10"),
      UserBrowseLog("user_1", "1501", "product_1", "10"),
      UserBrowseLog("user_1", "1502", "product_1", "10")
    )
      .assignAscendingTimestamps(_.eventTime.toLong*1000)
      .keyBy(_.userId)

    clickStream.intervalJoin(browseStream)
      .between(Time.minutes(-10),Time.seconds(0))
      .process(new IntervalJoinFunc)
      .print()

    /**
     * UserClickLog(user_1,1500,click,page_1) ==> UserBrowseLog(user_1,1000,product_1,10)
     * UserClickLog(user_1,1500,click,page_1) ==> UserBrowseLog(user_1,1500,product_1,10)
     * UserClickLog(user_1,2000,click,page_1) ==> UserBrowseLog(user_1,1500,product_1,10)
     * UserClickLog(user_1,2000,click,page_1) ==> UserBrowseLog(user_1,1501,product_1,10)
     * UserClickLog(user_1,2000,click,page_1) ==> UserBrowseLog(user_1,1502,product_1,10)
     */

    env.execute()
  }

  class IntervalJoinFunc extends ProcessJoinFunction[UserClickLog, UserBrowseLog, String] {
    override def processElement(left: UserClickLog, right: UserBrowseLog, ctx: ProcessJoinFunction[UserClickLog, UserBrowseLog, String]#Context, out: Collector[String]): Unit = {
      val str = s"${left} ==> ${right}"
      out.collect(str)
    }
  }

}
