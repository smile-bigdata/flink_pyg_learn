package com.itheima.batch_process

import com.itheima.batch_process.bean.{OrderRecord, OrderRecordWide}
import com.itheima.batch_process.task.{MerchantCountMoneyTask, PaymethodMoneyCountTask, PreprocessTask}
import com.itheima.batch_process.util.HBaseTableInputFormat
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object App {

  def main(args: Array[String]): Unit = {

    // 初始化批处理环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 并行度设置
    env.setParallelism(1)
    // 测试输出
    val textDataSet: DataSet[String] = env.fromCollection(List("1", "2", "3"))

    val tupleDataSet: DataSet[tuple.Tuple2[String, String]] = env.createInput(new HBaseTableInputFormat("mysql.pyg.orderRecord"))
    val orderRecordDataSet: DataSet[OrderRecord] = tupleDataSet.map {
      tuple2 => OrderRecord(tuple2.f1)
    }

    // 数据预处理,拓宽
    val wideDataSet: DataSet[OrderRecordWide] = PreprocessTask.process(orderRecordDataSet)

//    PaymethodMoneyCountTask.process(wideDataSet)

    MerchantCountMoneyTask.process(wideDataSet)
  }
}
