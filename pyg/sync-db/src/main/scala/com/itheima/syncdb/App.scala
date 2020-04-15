package com.itheima.syncdb

import com.itheima.syncdb.bean.{Canal, HBaseOperation}
import com.itheima.syncdb.task.PreprocessTask
import com.itheima.syncdb.util.{FlinkUtils, GlobalConfigUtil, HBaseUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.watermark.Watermark

object App {

  def main(args: Array[String]): Unit = {
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()

    // 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()

    // 测试打印
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)

    val canalDs: DataStream[Canal] = kafkaDataStream.map {
      json =>
        Canal(json)
    }
//    canalDs.print()

    val waterDs: DataStream[Canal] = canalDs.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Canal] {

      // 当前的时间戳
      var currentTimestamp = 0L

      // 延迟的时间
      val delayTime = 2000l

      // 返回水印时间
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimestamp - delayTime)
      }

      // 比较当前元素的时间和上一个元素的时间,取最大值,防止时光倒流
      override def extractTimestamp(element: Canal, previousElementTimestamp: Long): Long = {
        currentTimestamp = Math.max(element.timestamp, previousElementTimestamp)
        currentTimestamp
      }
    })
    waterDs.print()

    val hbaseDs: DataStream[HBaseOperation] = PreprocessTask.process(waterDs)
    hbaseDs.print()

    hbaseDs.addSink(new SinkFunction[HBaseOperation] {
      override def invoke(value: HBaseOperation): Unit = {
        value.opType match {
          case "DELETE" => HBaseUtil.deleteData(value.tableName,value.rowkey,value.cfName)
          case _ => HBaseUtil.putData(value.tableName,value.rowkey,value.cfName,value.colName,value.colValue)
        }
      }
    })

    // 执行任务
    env.execute("sync-db")
  }
}
