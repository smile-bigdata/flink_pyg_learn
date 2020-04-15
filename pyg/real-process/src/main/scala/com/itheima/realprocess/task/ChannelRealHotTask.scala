package com.itheima.realprocess.task

import com.itheima.realprocess.bean.ClickLogWide
import com.itheima.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow



case class ChannelRealHot(var channelid: String, var visited: Long)

/**
  * 频道热点分析
  *
  * 1. 字段转换
  * 2. 分组
  * 3. 时间窗口
  * 4. 聚合
  * 5. 落地HBase
  *
  */
object ChannelRealHotTask {

  def process(clickLogWideDataStream: DataStream[ClickLogWide]) = {

    // 1. 字段转换 channelid, visited
    val realHotDataStream: DataStream[ChannelRealHot] = clickLogWideDataStream.map {
      clickLogWide: ClickLogWide =>
        ChannelRealHot(clickLogWide.channelID, clickLogWide.count)
    }

    // 2. 分组
    val keyedStream: KeyedStream[ChannelRealHot, String] = realHotDataStream.keyBy(_.channelid)

    // 3. 时间窗口
    val windowedStream: WindowedStream[ChannelRealHot, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))

    // 4. 聚合
    val reduceDataStream: DataStream[ChannelRealHot] = windowedStream.reduce {
      (t1: ChannelRealHot, t2: ChannelRealHot) =>
        ChannelRealHot(t1.channelid, t1.visited + t2.visited)
    }

    // 5. 落地HBase
    reduceDataStream.addSink(new SinkFunction[ChannelRealHot] {

      override def invoke(value: ChannelRealHot): Unit = {

        // hbase相关字段
        val tableName = "channel"
        val clfName = "info"
        val channelIdColumn = "channelId"
        val visitedColumn = "visited"
        val rowkey = value.channelid

        // 查询HBase,获取相关记录
        val visitedValue: String = HBaseUtil.getData(tableName, rowkey, clfName, visitedColumn)
        // 创建总数的临时变量
        var totalCount: Long = 0

        if (StringUtils.isBlank(visitedValue)) {
          totalCount = value.visited
        } else {
          totalCount = visitedValue.toLong + value.visited
        }

        // 保存数据
        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
          channelIdColumn -> value.channelid,
          visitedColumn -> totalCount.toString
        ))
      }
    })
  }

}
