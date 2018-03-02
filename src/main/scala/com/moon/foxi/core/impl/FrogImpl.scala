package com.moon.foxi.core.impl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import com.moon.foxi.PartitionType
import com.moon.foxi.core.Frog
import org.apache.hadoop.mapred.TextInputFormat

/**
  * spark 2.0
  */
class FrogImpl(sparkSession: SparkSession) extends Frog {
  
  /**
    * 以 parquet 数据源创建表
    * @param table 注册的表名
    * @param path  源数据存储路径
    */
  override def createTable(table: String, path: String): Unit = {
    val df = sparkSession.read.parquet(path)
    df.createOrReplaceTempView(table)
  }

  /**
    * 以 parquet 数据源创建表
    * @param table         注册的表名
    * @param path          源数据存储路径
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  override def createTable(table: String, path: String, partitions: Int, partitionType: String): Unit = {
    var df = sparkSession.read.parquet(path)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    df.createOrReplaceTempView(table)
  }

  /**
    * 以 CSV 数据源创建表, 自动推测字段结构
    * @param table 注册的表名
    * @param path  源数据存储路径
    */
  override def createTableFromCSV(table: String, path: String): Unit = {
    val df = sparkSession.read.option("header", "true").csv(path)
    df.createOrReplaceTempView(table)
  }

  /**
    * 以 CSV 数据源创建表, 自动推测字段结构
    *
    * @param table         注册的表名
    * @param path          源数据存储路径
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  override def createTableFromCSV(table: String, path: String, partitions: Int, partitionType: String): Unit = {
    var df = sparkSession.read.option("header", "true").csv(path)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    df.createOrReplaceTempView(table)
  }

  /**
    * 以 CSV 数据源创建表, 根据提供的 desc 字段描述文件解析字段结构
    * @param table 注册的表名
    * @param path  源数据存储的路径
    * @param schema 表结构
    */
  override def createTableFromCSV(table: String, path: String, schema: StructType): Unit = {
    val df = sparkSession.read.schema(schema).csv(path)
    df.createOrReplaceTempView(table)
  }

  /**
    * 以 CSV 数据源创建表, 根据提供的 desc 字段描述文件解析字段结构
    * @param table         注册的表名
    * @param path          源数据存储的路径
    * @param schema        表结构
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  override def createTableFromCSV(table: String, path: String, schema: StructType, partitions: Int, partitionType: String): Unit = {
    var df = sparkSession.read.schema(schema).csv(path)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    df.createOrReplaceTempView(table)
  }

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table     注册的表名
    * @param path      源数据存储路径
    * @param schema    表结构
    * @param delimiter 分隔符
    * @param autoAdopt 自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    */
  override def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String, autoAdopt: Boolean): Unit = {

    this.createTableFromTXT(table, path, schema, delimiter, autoAdopt, "utf8")

  }

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table         注册的表名
    * @param path          源数据存储路径
    * @param schema        表结构
    * @param delimiter     分隔符
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param autoAdopt     自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    */
  override def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String, partitions: Int, partitionType: String, autoAdopt: Boolean): Unit = {

    this.createTableFromTXT(table, path, schema, delimiter, partitions, partitionType, autoAdopt, "utf8")

  }

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table     注册的表名
    * @param path      源数据存储路径
    * @param schema    表结构
    * @param delimiter 分隔符
    * @param autoAdopt 自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    * @param charset   字符编码
    */
  override def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String, autoAdopt: Boolean, charset: String): Unit = {

    var textRDD: RDD[String] = null

    if (charset == null  || charset.isEmpty || charset.toLowerCase == "utf8" || charset.toLowerCase == "utf-8") {
      textRDD = sparkSession.sparkContext.textFile(path)
    } else {
      textRDD = sparkSession.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path).map(line => {
       new String(line._2.getBytes, 0, line._2.getLength, charset)
      })
    }
    var rowRDD: RDD[Row] = null
    if (autoAdopt) {
      rowRDD = textRDD.map(row => Row(row.split(delimiter, schema.length).toSeq))
    } else {
      rowRDD = textRDD.map(row => Row(row.split(delimiter).toSeq))
    }
    val df = sparkSession.createDataFrame(rowRDD = rowRDD, schema = schema)
    df.createOrReplaceTempView(table)
  }

  /**
    * 以 TXT 数据源创建表, 根据提供的 desc 字段描述文件及分隔符解析数据
    * @param table         注册的表名
    * @param path          源数据存储路径
    * @param schema        表结构
    * @param delimiter     分隔符
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param autoAdopt     自动适配: 当字段数与描述文件字段不匹配时, 自动以描述文件为准, 可以解决当源数据追加字段时, 但与需要无关时自动适应变化
    * @param charset       字符编码
    */
  override def createTableFromTXT(table: String, path: String, schema: StructType, delimiter: String, partitions: Int, partitionType: String, autoAdopt: Boolean, charset: String): Unit = {

    var textRDD: RDD[String] = null

    if (charset == null  || charset.isEmpty || charset.toLowerCase == "utf8" || charset.toLowerCase == "utf-8") {
      textRDD = sparkSession.sparkContext.textFile(path)
    } else {
      textRDD = sparkSession.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path).map(line => {
        new String(line._2.getBytes, 0, line._2.getLength, charset)
      })
    }

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        textRDD = textRDD.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        textRDD = textRDD.repartition(partitions)
      } else {
        val currentNumPartitions = textRDD.partitions.length
        if (currentNumPartitions < partitions) {
          textRDD = textRDD.repartition(partitions)
        } else {
          textRDD = textRDD.coalesce(partitions)
        }
      }
    }

    var rowRDD: RDD[Row] = null
    if (autoAdopt) {
      rowRDD = textRDD.map(row => Row(row.split(delimiter, schema.length).toSeq))
    } else {
      rowRDD = textRDD.map(row => Row(row.split(delimiter).toSeq))
    }
    val df = sparkSession.createDataFrame(rowRDD = rowRDD, schema = schema)
    df.createOrReplaceTempView(table)
  }

  /**
    * 转换 SQL 执行
    * @param table 注册的表名
    * @param sql   转换SQL
    */
  override def transformTable(table: String, sql: String): Unit = {
    sparkSession.sql(sql).createOrReplaceTempView(table)
  }

  /**
    * 转换 SQL 执行
    * @param table         注册的表名
    * @param sql           转换SQL
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  override def transformTable(table: String, sql: String, partitions: Int, partitionType: String): Unit = {
    var df = sparkSession.sql(sql)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    df.createOrReplaceTempView(table)
  }

  /**
    * 转换 SQL 执行, 并将结果数据存储在中间仓储中, 此种方式可以减少长依赖时的资源使用情况
    * @param table 注册的表名
    * @param sql   转换SQL
    * @param path  中间数据存储路径
    */
  override def transformTable(table: String, sql: String, path: String): Unit = {
    sparkSession.sql(sql).createOrReplaceTempView(table)
    this.saveTable(table, path, "overwrite")
  }

  /**
    * 转换 SQL 执行, 并将结果数据存储在中间仓储中, 此种方式可以减少长依赖时的资源使用情况
    * @param table         注册的表名
    * @param sql           转换SQL
    * @param path          中间数据存储路径
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    */
  override def transformTable(table: String, sql: String, path: String, partitions: Int, partitionType: String): Unit = {
    var df = sparkSession.sql(sql)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }
    df.createOrReplaceTempView(table)

    this.saveTable(table, path, "overwrite")
  }

  /**
    * 将表结果存储为 parquet 数据
    * @param table 需要存储的表名
    * @param path  数据存储路径
    */
  override def saveTable(table: String, path: String, saveMode: String): Unit = {
    sparkSession.table(table).write.mode(saveMode).parquet(path)
  }

  /**
    * 将表结果存储为 parquet 数据
    * @param table         需要存储的表名
    * @param path          数据存储路径
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTable(table: String, path: String, partitions: Int, partitionType: String, saveMode: String): Unit = {
    var df = sparkSession.table(table)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    df.write.mode(saveMode).parquet(path)
  }

  /**
    * 将表结果存储为 parquet 数据
    * @param table       需要存储的表名
    * @param path        数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTable(table: String, path: String, partitionBy: String, saveMode: String): Unit = {

    if (partitionBy.isEmpty) {
      sparkSession.table(table).write.mode(saveMode).parquet(path)
    } else {
      sparkSession.table(table).write.mode(saveMode).partitionBy(partitionBy).parquet(path)
    }
  }

  /**
    * 将表结果存储为 parquet 数据
    * @param table         需要存储的表名
    * @param path          数据存储路径
    * @param partitionBy   分区字段: 多个以 `逗号(,)` 分割
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTable(table: String, path: String, partitionBy: String, partitions: Int, partitionType: String, saveMode: String): Unit = {
    var df = sparkSession.table(table)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    if (partitionBy.isEmpty) {
      df.write.mode(saveMode).parquet(path)
    } else {
      df.write.partitionBy(partitionBy).mode(saveMode).parquet(path)
    }
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table 需要存储的表名
    * @param path  数据存储路径
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsCSV(table: String, path: String, saveMode: String): Unit = {
    sparkSession.table(table).write.mode(saveMode).csv(path)
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table         需要存储的表名
    * @param path          数据存储路径
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsCSV(table: String, path: String, partitions: Int, partitionType: String, saveMode: String): Unit = {
    var df = sparkSession.table(table)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    df.write.mode(saveMode).csv(path)
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table       需要存储的表名
    * @param path        数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsCSV(table: String, path: String, partitionBy: String, saveMode: String): Unit = {

    if (partitionBy.isEmpty) {
      sparkSession.table(table).write.mode(saveMode).csv(path)
    } else {
      sparkSession.table(table).write.partitionBy(partitionBy).mode(saveMode).csv(path)
    }
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table         需要存储的表名
    * @param path          数据存储路径
    * @param partitionBy   分区字段: 多个以 `逗号(,)` 分割
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsCSV(table: String, path: String, partitionBy: String, partitions: Int, partitionType: String, saveMode: String): Unit = {
    var df = sparkSession.table(table)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    if (partitionBy.isEmpty) {
      df.write.mode(saveMode).csv(path)
    } else {
      df.write.partitionBy(partitionBy).mode(saveMode).csv(path)
    }
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table     需要存储的表名
    * @param path      数据存储路径
    * @param delimiter 列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsTXT(table: String, path: String, delimiter: String, saveMode: String): Unit = {

    import sparkSession.implicits._
    sparkSession.table(table).map(_.mkString(delimiter)).write.mode(saveMode).text(path)
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table         需要存储的表名
    * @param path          数据存储路径
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param delimiter 列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsTXT(table: String, path: String, partitions: Int, partitionType: String, delimiter: String, saveMode: String): Unit = {
    var df = sparkSession.table(table)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    import sparkSession.implicits._
    df.map(_.mkString(delimiter)).write.mode(saveMode).text(path)
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table       需要存储的表名
    * @param path        数据存储路径
    * @param partitionBy 分区字段: 多个以 `逗号(,)` 分割
    * @param delimiter   列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsTXT(table: String, path: String, partitionBy: String, delimiter: String, saveMode: String): Unit = {
    val df = sparkSession.table(table)

    import sparkSession.implicits._
    if (partitionBy.isEmpty) {
      df.map(_.mkString(delimiter)).write.mode(saveMode).text(path)
    } else {
      df.map(_.mkString(delimiter)).write.partitionBy(partitionBy).mode(saveMode).text(path)
    }
  }

  /**
    * 将表结果存储为 CSV 数据
    * @param table         需要存储的表名
    * @param path          数据存储路径
    * @param partitionBy   分区字段: 多个以 `逗号(,)` 分割
    * @param partitions    重分区数
    * @param partitionType 分区类型: coalesce repartition partition[自动分区]
    * @param delimiter 列分隔符
    * @param saveMode 存储方式: overwrite append ignore etc.
    */
  override def saveTableAsTXT(table: String, path: String, partitionBy: String, partitions: Int, partitionType: String, delimiter: String, saveMode: String): Unit = {

    var df = sparkSession.table(table)

    if (partitions > 0) {
      if (partitionType == PartitionType.COALESCE) {
        df = df.coalesce(partitions)
      } else if (partitionType == PartitionType.REPARTITION) {
        df = df.repartition(partitions)
      } else {
        val currentNumPartitions = df.rdd.partitions.length
        if (currentNumPartitions < partitions) {
          df = df.repartition(partitions)
        } else {
          df = df.coalesce(partitions)
        }
      }
    }

    import sparkSession.implicits._
    if (partitionBy.isEmpty) {
      df.map(_.mkString(delimiter)).write.mode(saveMode).text(path)
    } else {
      df.map(_.mkString(delimiter)).write.partitionBy(partitionBy).mode(saveMode).text(path)
    }
  }

  /**
    * 缓存表
    * @param table 表名
    * @return
    */
  override def cacheTable(table: String): Boolean = {
    val isCache = sparkSession.catalog.isCached(table)
    if (!isCache) {
      sparkSession.catalog.cacheTable(table)
    } else {
      System.err.println(s"table $table is already cached.")
    }

    true
  }

  /**
    * 移除表缓存
    * @param table 表名
    * @return
    */
  override def uncacheTable(table: String): Boolean = {
    val isCache = sparkSession.catalog.isCached(table)
    if (isCache) {
      sparkSession.catalog.uncacheTable(table)
    } else {
      System.err.println(s"table $table is already uncached.")
    }

    true
  }

  /**
    * 打印一定条数的表数据, 最大 1000 行
    * @param table 表名
    * @param count 打印行数
    */
  def print(sparkSession: SparkSession,table: String, count: Int): Unit = {
    val n = if (count > 1000) 1000 else count
     sparkSession.table(table).take(n).foreach(println)
  }

  /**
    * 打印一定条数的表数据, 最大 1000 行
    * @param df     DataFrame
    * @param count  打印行数
    */
  def print(df: DataFrame, count: Int): Unit = {
    val n = if (count > 1000) 1000 else count
    df.take(n).foreach(println)
  }

  override def hello(): String = {
    "Hello, Frog!"
  }
}
