package myutils

import java.text.SimpleDateFormat
import java.util.Date

object AYTime {
  def formatTS(getEnd: Long): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(new Date(getEnd*1000))
  }
}
