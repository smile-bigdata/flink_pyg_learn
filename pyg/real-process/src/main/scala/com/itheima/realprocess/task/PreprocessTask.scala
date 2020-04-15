package com.itheima.realprocess.task

import com.itheima.realprocess.bean.{ClickLogWide, Message}
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

/**
  * 预处理任务
  */
object PreprocessTask {

  def process(watermarkDataStrem: DataStream[Message]): DataStream[ClickLogWide] = {

    watermarkDataStrem.map {
      msg =>
        // 转换时间
        val yearMonth: String = FastDateFormat.getInstance("yyyyMM").format(msg.timeStamp)
        val yearMonthDay: String = FastDateFormat.getInstance("yyyyMMdd").format(msg.timeStamp)
        val yearMonthDayHour: String = FastDateFormat.getInstance("yyyyMMddHH").format(msg.timeStamp)


        // 转换地区
        val address = msg.clickLog.country + msg.clickLog.province + msg.clickLog.city


        val isNewTuple: (Int, Int, Int, Int) = isNewProcess(msg)

        ClickLogWide(

          msg.clickLog.channelID,
          msg.clickLog.categoryID,
          msg.clickLog.produceID,
          msg.clickLog.country,
          msg.clickLog.province,
          msg.clickLog.city,
          msg.clickLog.network,
          msg.clickLog.source,
          msg.clickLog.browserType,
          msg.clickLog.entryTime,
          msg.clickLog.leaveTime,
          msg.clickLog.userID,
          msg.count,
          msg.timeStamp,
          address,
          yearMonth,
          yearMonthDay,
          yearMonthDayHour,
          isNewTuple._1,
          isNewTuple._2,
          isNewTuple._3,
          isNewTuple._4

        )
    }
  }

  /**
    * 判断用户是否为新用户
    *
    * @param msg
    */
  private def isNewProcess(msg: Message) = {

    // 1. 定义4个变量,初始化为0
    var isNew = 0
    var isHourNew = 0
    var isDayNew = 0
    var isMonthNew = 0

    // 2. 从HBase中查询用户记录,如果有记录,再去判断其它时间,如果没记录,则证明是新用户

    val tableName = "user_history"
    var clfName = "info"
    var rowkey = msg.clickLog.userID + ":" + msg.clickLog.channelID

    //    用户ID(userid)
    var userIdColumn = "userid"
    //    频道ID(channelid)
    var channelidColumn = "channelid"
    //    最后访问时间（时间戳）(lastVisitedTime)
    var lastVisitedTimeColumn = "lastVisitedTime"

    val userId: String = HBaseUtil.getData(tableName, rowkey, clfName, userIdColumn)
    val channelid: String = HBaseUtil.getData(tableName, rowkey, clfName, channelidColumn)
    val lastVisitedTime: String = HBaseUtil.getData(tableName, rowkey, clfName, lastVisitedTimeColumn)

    // 如果userid为空,则该用户一定是新用户
    if (StringUtils.isBlank(userId)) {
      isNew = 1
      isHourNew = 1
      isDayNew = 1
      isMonthNew = 1

      // 保存用户的访问记录到'user_history'
      HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
        userIdColumn -> msg.clickLog.userID,
        channelidColumn -> msg.clickLog.channelID,
        lastVisitedTimeColumn -> msg.timeStamp
      ))

    } else {
      isNew = 0
      // 其它字段需要进行时间戳的比对  201912 > 201911
      isHourNew = compareDate(msg.timeStamp,lastVisitedTime.toLong,"yyyyMMddHH")
      isDayNew = compareDate(msg.timeStamp,lastVisitedTime.toLong,"yyyyMMdd")
      isMonthNew = compareDate(msg.timeStamp,lastVisitedTime.toLong,"yyyyMM")

      // 更新'user_history'的用户的时间戳
      HBaseUtil.putData(tableName, rowkey, clfName,lastVisitedTimeColumn , msg.timeStamp.toString)
    }

    (isNew, isHourNew, isDayNew, isMonthNew)
  }

  /**
    * 比对时间
    * @param currentTime    当前时间
    * @param historyTime    历史时间
    * @param format         时间格式 yyyyMM yyyyMMdd
    * @return               1或者0
    */
  def compareDate(currentTime:Long,historyTime:Long,format:String):Int={

    val currentTimeStr: String = timestamp2Str(currentTime,format)
    val historyTimeStr: String = timestamp2Str(historyTime,format)

    // 比对字符串大小,如果当前时间>历史时间 返回1

    var result: Int = currentTimeStr.compareTo(historyTimeStr)

    if(result>0){
      result = 1
    }else{
      result = 0
    }

    result

  }

  /**
    * 转换日期
    * @param timestamp    Long类型的时间戳
    * @param format       日期格式
    * @return
    */
  def timestamp2Str(timestamp:Long,format:String): String ={
    FastDateFormat.getInstance(format).format(timestamp)
  }


}
