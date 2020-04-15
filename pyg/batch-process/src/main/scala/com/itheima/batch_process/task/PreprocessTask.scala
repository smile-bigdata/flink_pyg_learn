package com.itheima.batch_process.task

import com.itheima.batch_process.bean.{OrderRecord, OrderRecordWide}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

object PreprocessTask {

  def process(orderRecordDataSet: DataSet[OrderRecord]) = {

    orderRecordDataSet.map {
      orderRecord =>
        OrderRecordWide(
          orderRecord.benefitAmount,
          orderRecord.orderAmount,
          orderRecord.payAmount,
          orderRecord.activityNum,
          orderRecord.createTime,
          orderRecord.merchantId,
          orderRecord.orderId,
          orderRecord.payTime,
          orderRecord.payMethod,
          orderRecord.voucherAmount,
          orderRecord.commodityId,
          orderRecord.userId,
          formatTime(orderRecord.createTime,"yyyyMMdd"),
          formatTime(orderRecord.createTime,"yyyyMM"),
          formatTime(orderRecord.createTime,"yyyy")

        )
    }

  }

  // 2018-08-13 00:00:06 => 时间戳 => 年月日 或者 年月 或者 年
  def formatTime(date:String,format:String)={

    val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val timeStamp: Long = dateFormat.parse(date).getTime

    FastDateFormat.getInstance(format).format(timeStamp)

  }

}
