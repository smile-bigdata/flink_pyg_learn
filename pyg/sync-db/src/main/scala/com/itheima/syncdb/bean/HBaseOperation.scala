package com.itheima.syncdb.bean

/**
  * 操作类型（opType）= INSERT/DELETE/UPDATE
  * 表名（tableName）= mysql.binlog数据库名.binlog表名
  * 列族名（cfName）= 固定为info
  * rowkey = 唯一主键（取binlog中列数据的第一个）
  * 列名（colName）= binlog中列名
  * 列值（colValue）= binlog中列值
  */
case class HBaseOperation(
                           var opType: String,
                           val tableName: String,
                           val cfName: String,
                           val rowkey: String,
                           val colName: String,
                           val colValue: String
                         )
