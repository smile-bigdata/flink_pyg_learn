package com.itheima.batch_process.task

import com.itheima.batch_process.bean.OrderRecordWide
import com.itheima.batch_process.util.HBaseUtil
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._

/**
  * 1. 创建样例类 MerchantCountMoney
  * 商家ID, 时间,支付金额,订单数量,
  *
  * 2. 编写process方法
  *
  * 转换  flatmap
  *
  * 分组 groupby
  *
  * 聚合 reduce
  *
  * 落地保存到HBase
  *
  *
  */

case class MerchantCountMoney(
                               var merchantId: String,
                               var date: String,
                               var amount: Double,
                               var count: Long
                             )

object MerchantCountMoneyTask {

  def process(wideDataSet: DataSet[OrderRecordWide]) = {

    //转换  flatmap
    val mapDataSet: DataSet[MerchantCountMoney] = wideDataSet.flatMap {
      orderRecordWide => {
        List(
          MerchantCountMoney(orderRecordWide.merchantId, orderRecordWide.yearMonthDay, orderRecordWide.payAmount.toDouble, 1),
          MerchantCountMoney(orderRecordWide.merchantId, orderRecordWide.yearMonth, orderRecordWide.payAmount.toDouble, 1),
          MerchantCountMoney(orderRecordWide.merchantId, orderRecordWide.year, orderRecordWide.payAmount.toDouble, 1)
        )
      }
    }

    //分组 groupby
    val groupDataSet: GroupedDataSet[MerchantCountMoney] = mapDataSet.groupBy {
      merchant => (merchant.merchantId + merchant.date)
    }

    // 聚合
    val reduceDataSet: DataSet[MerchantCountMoney] = groupDataSet.reduce {
      (p1, p2) => {
        MerchantCountMoney(p1.merchantId, p1.date, p1.amount + p2.amount, p1.count + p2.count)
      }
    }

    // 保存到HBase中
    reduceDataSet.collect().foreach {
      merchant => {
        // HBase相关字段

        val tableName = "analysis_merchant"
        val rowkey = merchant.merchantId + ":" + merchant.date
        val clfName = "info"

        val merchantIdColumn = "merchantId"
        val dateColumn = "date"
        val amountColumn = "amount"
        val countColumn = "count"

        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
          merchantIdColumn -> merchant.merchantId,
          dateColumn -> merchant.date,
          amountColumn -> merchant.amount,
          countColumn -> merchant.count

        ))

      }
    }

  }

}
