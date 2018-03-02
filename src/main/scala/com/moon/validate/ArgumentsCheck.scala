package com.moon.validate

import java.io.File

import com.moon.models._

/// TODO: 检查文件参数规范性
object ArgumentsCheck {

  def configParamsChcek(info: TransformInfo): Boolean = {

    var result = true

    val sourceTables = info.sources
    val transTables = info.transforms
    val sinkTables = info.sinks

    // Warning检查

    // 错误检查

    // 检查1: source && transform 表均唯一
    val tables = sourceTables.map(t => (t.tableName,1)) ++ transTables.map(t => (t.tableName, 1))
    val repeatedTables = tables.groupBy(_._1).filter(_._2.length > 1)
    repeatedTables.foreach(t => {
      System.err.println(s"表: ${t._1} 在 source 与 transform 中存在重复，请检查！")
      result = false
    })

    // 2.当数据源的数据类型为 txt 格式时，必须包含字段映射文件
    sourceTables.filter(_.dataType.toLowerCase() == "txt").foreach(t => {
      if (!new File(t.metaPath).isFile) {
        System.err.println(s"源数据表: ${t.tableName} 在的映射文件 ${t.metaPath} 不存在，请检查！")
        result = false
      }
    })

    // 3.Sink的数据表必须在 source 与 transform 中存在;
    // 出打印模式外, 所有的Sink表必须存在保存路径
    sinkTables.foreach(t => {
      if (t.tableName.isEmpty) {
        System.err.println(s"Sink表: 表名为空，请检查！")
        result = false
      }

      if (!tables.exists(_._1 == t.tableName)) {
        System.err.println(s"Sink表: ${t.tableName} 在 source 与 transform 中不存在，请检查！")
        result = false
      }

      if (t.print == 0 && t.savePath.isEmpty) {
        System.err.println(s"Sink表: ${t.tableName} 无保存路径，请检查！")
        result = false
      }
    })


    result
  }

  /**
    * source conf check
    * @param source
    * @return 返回码: [1000 - 1999]
    */
  def sourceCheck(source: SourceConf) : Boolean = {
    true
  }

  /**
    * transform conf check
    * @param transform
    * @return 返回码: [2000 - 2999]
    */
  def transformCheck(transform: TransformConf) : Boolean = {
    true
  }

  /**
    * sink conf check
    * @param sink
    * @return 返回码: [3000 - 3999]
    */
  def sinkCheck(sink: SinkConf) : Boolean = {
    true
  }

  /**
    * schema check
    * @param schema
    * @return 返回码: [4000 - 4999]
    */
  def schemaCheck(schema: SchemaInfo) : Boolean = {
    true
  }

}
