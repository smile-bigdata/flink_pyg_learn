package com.itheima.realprocess

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.itheima.realprocess.bean.{ClickLog, ClickLogWide, Message}
import com.itheima.realprocess.task._
import com.itheima.realprocess.util.GlobalConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object App {

  def main(args: Array[String]): Unit = {

    // ---------------初始化FLink的流式环境--------------
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置处理的时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 设置并行度
    env.setParallelism(1)

    // 本地测试 加载本地集合 成为一个DataStream 打印输出

        val localDataStream: DataStream[String] = env.fromCollection(
          List("hadoop", "hive", "hbase", "flink")
        )
        localDataStream.print()


    // 添加Checkpoint的支持
    // 5s钟启动一次checkpoint
    env.enableCheckpointing(5000)

    // 设置checkpoint只checkpoint一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 最大并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭时,触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink-checkpoint/"))


    // -------------整合Kafka----------
    val properties = new Properties()

    //    # Kafka集群地址
    properties.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    //    # ZooKeeper集群地址
    properties.setProperty("zookeeper.connect", GlobalConfigUtil.zookeeperConnect)
    //    # Kafka Topic名称
    properties.setProperty("input.topic", GlobalConfigUtil.inputTopic)
    //    # 消费组ID
    properties.setProperty("group.id", GlobalConfigUtil.groupId)
    //    # 自动提交拉取到消费端的消息offset到kafka
    properties.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    //    # 自动提交offset到zookeeper的时间间隔单位（毫秒）
    properties.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    //    # 每次消费最新的数据
    properties.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)


    // 话题 反序列化器 属性集合
    val consumer = new FlinkKafkaConsumer010[String](GlobalConfigUtil.inputTopic, new SimpleStringSchema(), properties)

    val kafkaDatastream: DataStream[String] = env.addSource(consumer)

    //    kafkaDatastream.print()

    // JSON -> 元组
    val tupleDataStream = kafkaDatastream.map {
      msgJson =>
        val jsonObject = JSON.parseObject(msgJson)

        val message = jsonObject.getString("message")
        val count = jsonObject.getLong("count")
        val timeStamp = jsonObject.getLong("timeStamp")

        //        (message,count,timeStamp)

        //        (ClickLog(message),count,timeStamp)
        Message(ClickLog(message), count, timeStamp)
    }


    //    tupleDataStream.print()

    // -----------添加水印支持--------------------

    var watermarkDataStrem = tupleDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {
      var currentTimeStamp = 0l

      // 延迟时间
      var maxDelayTime = 2000l

      // 获取当前时间戳
      override def getCurrentWatermark: Watermark = {

        new Watermark(currentTimeStamp - maxDelayTime)
      }

      // 获取事件时间
      override def extractTimestamp(element: Message, previousElementTimestamp: Long): Long = {

        currentTimeStamp = Math.max(element.timeStamp, previousElementTimestamp)
        currentTimeStamp
      }
    })

    // 数据的预处理
    val clickLogWideDataStream: DataStream[ClickLogWide] = PreprocessTask.process(watermarkDataStrem)

    //    clickLogWideDataStream.print()
    //    ChannelRealHotTask.process(clickLogWideDataStream)
    //    ChannelPvUvTask.process(clickLogWideDataStream)
    //    ChannelFreshnessTask.process(clickLogWideDataStream)
    //    ChannelAreaTask.process(clickLogWideDataStream)

    //    ChannelNetWorkTask.process(clickLogWideDataStream)

    ChannelBrowserTask.process(clickLogWideDataStream)

    // 执行任务
    env.execute("real-process")

  }


}
