package com.moon.models

class TransformInfo {

  /**
    * pipe conf
    */
  var pipe = new PipeConf

  /**
    * source conf
    */
  var sources: List[SourceConf] = List()

  /**
    * transform conf
    */
  var transforms: List[TransformConf] = List()

  /**
    * sink conf
    */
  var sinks: List[SinkConf] = List()

  override def toString = s"TransformInfo($pipe, $sources, $transforms, $sinks)"
}

class PipeConf {
  /**
    * 中间表数据存放路径
    * HDFS
    */
  var tempPath = ""

  /**
    * 执行日志存：保存执行任务时的任务提交日志，方便了解任务执行情况，存放与本地文件系统
    * LOCAL
    */
  var logPath = ""

  /**
    * Schema存放文件夹
    * LOCAL
    */
  var schemaPath = ""

  /**
    * 元数据存放文件夹
    * LOCAL
    */
  var metaPath = ""

  /**
    * 元数据的数据库名称
    */
  var dbName = ""

  override def toString = s"PipeConf(tempPath=$tempPath, logPath=$logPath, metaPath=$metaPath)"
}

class SourceConf {
  /**
    * 表名：自己定义的表名，方便 transfrom 使用
    */
  var tableName = ""

  /**
    * 数据类型：如 parquet csv 等
    */
  var dataType = ""

  /**
    * 数据存储位置：HDFS 上数据的具体位置;
    * ps: local模式下可以使用 本地文件系统
    */
  var dataPath = ""

  /**
    * 数据 meta 描述文件: 如果文件为 parquet 等类型数据文件, 此参数将无效
    */
  var metaPath = ""

  /**
    * 打印数据行数，用于检查数据是否正常加载
    */
  var print = 0

  /**
    * 分区数
    */
  var partitions = 0

  /**
    * 分区实现方式: coalesce repartition autopartitions
    */
  var partitionType = "coalesce"

  /**
    * StorageLevel.MEMORY_ONLY
    */
  var cache = false

  /**
    * StorageLevel.MEMORY_DISK...
    */
  var persist = false

  /**
    * 数据索引,确定数据顺序位置信息
    */
  var index = 0

  /**
    * 是否是下沉表
    */
  var isSink = false

  def printInfo() = {
    System.out.println(s"[index：${index}]")
    System.out.println(s"[tableName：${tableName}]")
    System.out.println(s"[dataPath：${dataPath}]")
    System.out.println(s"[dataType：${dataType}]")
    System.out.println(s"[metaPath：${metaPath}]")
    System.out.println(s"[cache：${cache}]")
    System.out.println(s"[persist：${persist}]")
    System.out.println(s"[partitions：${partitions} - ${partitionType}]")
    System.out.println(s"[print：${print}]")
  }
}

class TransformConf {
  /**
    * 表名：自己定义的表名，方便 transfrom 使用
    */
  var tableName = ""

  /**
    * 数据类型：如 parquet csv 等
    */
  var dataType = ""

  /**
    * StorageLevel.MEMORY_ONLY
    */
  var cache = false

  /**
    * StorageLevel.MEMORY_DISK...
    * 如果 cache && persist 同时为 true 时, persist 优先于 cache
    */
  var persist = false

  /**
    * 分区数
    */
  var partitions = 0

  /**
    * 分区实现方式: coalesce repartition autopartitions
    */
  var partitionType = "coalesce"

  /**
    * 转换SQL
    */
  var sparkSql = ""

  /**
    * 中间数据保存
    */
  var save = false

  /**
    * 打印数据行数，用于检查数据是否正常加载
    */
  var print = 0

  /**
    * 数据索引,确定数据顺序位置信息
    */
  var index = 0

  /**
    * 是否是下沉表
    */
  var isSink = false

  def printInfo() = {
    System.out.println(s"[index：${index}]")
    System.out.println(s"[save：${save}]")
    System.out.println(s"[tableName：${tableName}]")
    System.out.println(s"[dataType：${dataType}]")
    System.out.println(s"[cache：${cache}]")
    System.out.println(s"[persist：${persist}]")
    System.out.println(s"[partitions：${partitions} - ${partitionType}]")
    System.out.println(s"[print：${print}]")
    System.out.println(s"[sparkSql：${sparkSql}]")
  }
}

class SinkConf {
  /**
    * 表名：自己定义的表名，方便 transfrom 使用
    */
  var tableName = ""

  /**
    * 数据类型：如 parquet csv 等
    */
  var dataType = ""

  /**
    * 分区字段
    */
  var partitionBy = ""

  /**
    * 分区数
    */
  var partitions = 0

  /**
    * 分区实现方式: coalesce repartition autopartitions
    */
  var partitionType = "coalesce"

  /**
    * 数据落地模式：
    * overwrite:重写，将数据写入磁盘；append：追加
    */
  var saveMode = "overwrite"

  /**
    * 数据存储路径
    */
  var savePath = ""

  /**
    * 数据分隔符：当数据格式为 文本类型时有效，如 csv,text等格式
    * 默认 ,
    */
  var delimiter = ","

  /**
    * gzip压缩
    */
  var gzip = false

  /**
    * 打印数据行数，用于检查数据是否正常加载
    * 默认：0 条
    */
  var print = 0

  /**
    * 数据索引,确定数据顺序位置信息
    */
  var index = 0

  /**
    * 存储方式: print_only save_only save_and_print
    */
  var saveType = ""

  def printInfo() = {
    System.out.println(s"[index：${index}]")
    System.out.println(s"[tableName：${tableName}]")
    System.out.println(s"[savePath：${savePath}]")
    System.out.println(s"[saveMode：${saveMode}]")
    System.out.println(s"[dataType：${dataType}]")
    System.out.println(s"[delimiter：${delimiter}]")
    System.out.println(s"[partitionBy：${partitionBy}]")
    System.out.println(s"[partitions：${partitions} - ${partitionType}]")
    System.out.println(s"[print：${print}]")
  }
}