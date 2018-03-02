package com.moon.foxi.core

import com.moon.foxi.core.impl.FrogImpl
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark 2.0
  */
trait Frog {

  // source data 注册

  /**
    * 以 parquet 数据源创建表
    * @param table 注册的表名
    * @param path 源数据存储路径
    */
  def createTable(table: String, path: String): Unit

  /**
    * 以 parquet 数据源创建表
    * @param table 注册的表名
    * @param path 源数据存储路径
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  def createTable(table: String, path: String, partitions: Int, partitionType: String): Unit

  /**
    * 以 CSV 数据源创建表, 自动推测字段结构
    * @param table 注册的表名
    * @param path 源数据存储路径
    */
  def createTableFromCSV(table: String, path: String): Unit

  /**
    * 以 CSV 数据源创建表, 自动推测字段结构
    * @param table 注册的表名
    * @param path 源数据存储路径
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  def createTableFromCSV(table: String, path: String, partitions: Int, partitionType: String): Unit

  /**
    * 以 CSV 数据源创建表, 根据提供的 desc 字段描述文件解析字段结构
    * @param table 注册的表名
    * @param path 源数据存储的路径
    * @param schema 表结构
    */
  def createTableFromCSV(table: String, path: String, schema: StructType): Unit

  /**
    * 以 CSV 数据源创建表, 根据提供的 desc 字段描述文件解析字段结构
    * @param table 注册的表名
    * @param path 源数据存储的路径
    * @param schema 表结构
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  def createTableFromCSV(table: String, path: String, schema: StructType, partitions: Int, partitionType: String): Unit

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table 注册的表名
    * @param path 源数据存储路径
    * @param schema 表结构
    * @param delimiter 分隔符
    * @param autoAdopt 自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    */
  def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String, autoAdopt: Boolean): Unit

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table 注册的表名
    * @param path 源数据存储路径
    * @param schema 表结构
    * @param delimiter 分隔符
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param autoAdopt 自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    */
  def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String,
                         partitions: Int, partitionType: String, autoAdopt: Boolean): Unit

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table 注册的表名
    * @param path 源数据存储路径
    * @param schema 表结构
    * @param delimiter 分隔符
    * @param autoAdopt 自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    * @param charset   字符编码
    */
  def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String, autoAdopt: Boolean, charset: String): Unit

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table 注册的表名
    * @param path 源数据存储路径
    * @param schema 表结构
    * @param delimiter 分隔符
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param autoAdopt 自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    * @param charset   字符编码
    */
  def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String,
                         partitions: Int, partitionType: String, autoAdopt: Boolean, charset: String): Unit

  // transformation 转换

  /**
    * 转换 SQL 执行
    * @param table 注册的表名
    * @param sql 转换SQL
    */
  def transformTable(table: String, sql: String): Unit

  /**
    * 转换 SQL 执行
    * @param table 注册的表名
    * @param sql 转换SQL
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  def transformTable(table: String, sql: String, partitions: Int, partitionType: String): Unit

  /**
    * 转换 SQL 执行, 并将结果数据存储在中间仓储中, 此种方式可以减少长依赖时的资源使用情况
    * @param table 注册的表名
    * @param sql 转换SQL
    * @param path 中间数据存储路径
    */
  def transformTable(table: String, sql: String, path: String): Unit

  /**
    * 转换 SQL 执行, 并将结果数据存储在中间仓储中, 此种方式可以减少长依赖时的资源使用情况
    * @param table 注册的表名
    * @param sql 转换SQL
    * @param path 中间数据存储路径
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  def transformTable(table: String, sql: String, path: String, partitions: Int, partitionType: String): Unit

  // data 存储: txt, csv, parquet

  /**
    * 将表结果存储为 parquet 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTable(table: String, path: String, saveMode: String): Unit

  /**
    * 将表结果存储为 parquet 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTable(table: String, path: String, partitions: Int, partitionType: String, saveMode: String): Unit

  /**
    * 将表结果存储为 parquet 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTable(table: String, path: String, partitionBy: String, saveMode: String): Unit

  /**
    * 将表结果存储为 parquet 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTable(table: String, path: String, partitionBy: String, partitions: Int, partitionType: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsCSV(table: String, path: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsCSV(table: String, path: String, partitions: Int, partitionType: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsCSV(table: String, path: String, partitionBy: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsCSV(table: String, path: String, partitionBy: String, partitions: Int, partitionType: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param delimiter 列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsTXT(table: String, path: String, delimiter: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param delimiter 列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsTXT(table: String, path: String, partitions: Int, partitionType: String, delimiter: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param delimiter 列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsTXT(table: String, path: String, partitionBy: String, delimiter: String, saveMode: String): Unit

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path 数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param partitions 重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param delimiter 列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  def saveTableAsTXT(table: String, path: String, partitionBy: String, partitions: Int, partitionType: String, delimiter: String, saveMode: String): Unit

  // cache table: used catalog.cacheTable, it's a lazy function, tigger with action.
  /**
    * 缓存表
    * @param table 表名
    * @return
    */
  def cacheTable(table: String): Boolean

  // uncache table: used catalog.uncacheTable, it's a lazy function, tigger with action.
  /**
    * 移除表缓存
    * @param table 表名
    * @return
    */
  def uncacheTable(table: String): Boolean

  /**
    * 打印一定条数的表数据, 最大 1000 行
    * @param sparkSession SparkSession
    * @param table 表名
    * @param count 打印行数
    */
  def print(sparkSession: SparkSession,table: String, count: Int): Unit

  /**
    * 打印一定条数的表数据, 最大 1000 行
    * @param df     DataFrame
    * @param count  打印行数
    */
  def print(df: DataFrame, count: Int): Unit

  def hello(): String

}

object Frog {

  /**
    * 实例化 Frog
    * @param sparkSession SparkSession
    * @return FrogImpl    实例化
    */
  def apply (sparkSession: SparkSession): Frog = {
    new FrogImpl(sparkSession)
  }

}
