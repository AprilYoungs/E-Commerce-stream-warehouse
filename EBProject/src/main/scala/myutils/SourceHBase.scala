package myutils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.ConnectionFactory

object SourceHBase {

  def getConnection() = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "centos7-2,centos7-1")
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    ConnectionFactory.createConnection(conf)
  }
}
