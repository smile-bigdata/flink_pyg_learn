package com.itheima.batch_process.task

import com.itheima.batch_process.bean.OrderRecordWide
import com.itheima.batch_process.util.HBaseUtil
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

case class PaymethodMoneyCount(
                                var payMethod: String, // 支付方式
                                var date: String, //日期
                                var moneyCount: Double, //订单金额
                                var count: Long //订单总数
                              )

object PaymethodMoneyCountTask {

  def process(orderRecordWideDataSet: DataSet[OrderRecordWide]) = {

    // 1. 转换
    val payMethodDataSet: DataSet[PaymethodMoneyCount] = orderRecordWideDataSet.flatMap {
      orderRecordWide =>
        List(
          PaymethodMoneyCount(orderRecordWide.payMethod, orderRecordWide.yearMonthDay, orderRecordWide.payAmount.toDouble, 1),
          PaymethodMoneyCount(orderRecordWide.payMethod, orderRecordWide.yearMonth, orderRecordWide.payAmount.toDouble, 1),
          PaymethodMoneyCount(orderRecordWide.payMethod, orderRecordWide.year, orderRecordWide.payAmount.toDouble, 1)
        )
    }

    // 2. 分组

    val groupDataSet: GroupedDataSet[PaymethodMoneyCount] = payMethodDataSet.groupBy {
      paymethodMoneyCount => paymethodMoneyCount.payMethod + paymethodMoneyCount.date
    }


    // 3. 聚合

    val reduceDataSet: DataSet[PaymethodMoneyCount] = groupDataSet.reduce {
      (p1, p2) =>
        PaymethodMoneyCount(p1.payMethod, p1.date, p1.moneyCount + p2.moneyCount, p1.count + p2.count)
    }


    // 4. 落地

    reduceDataSet.collect().foreach {
      paymethodMoneyCount =>
        // 构造HBase相关列
        val tableName = "analysis_payment"
        val clfName = "info"
        val rowkey = paymethodMoneyCount.payMethod + ":" + paymethodMoneyCount.date

        var payMethodColumn = "payMethod"
        var dateColumn = "date"
        var moneyCountColumn = "moneyCount"
        var countColumn = "count"

        HBaseUtil.putMapData(
          tableName, rowkey, clfName, Map(
            payMethodColumn -> paymethodMoneyCount.payMethod,
            dateColumn -> paymethodMoneyCount.date,
            moneyCountColumn -> paymethodMoneyCount.moneyCount,
            countColumn -> paymethodMoneyCount.count
          )
        )


    }


  }


}
