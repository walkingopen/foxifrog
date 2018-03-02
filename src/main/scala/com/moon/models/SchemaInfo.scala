package com.moon.models

/**
  * Meta file table info.
  */
class SchemaInfo {

  /**
    * 字符集
    */
  var charset = "UTF-8"

  /**
    * 分隔符
    */
  var delimiter = ","

  /**
    * 数据格式
    */
  var dataType = "xsv"

  /**
    * 数据格式
    */
  var isHeaderFirst = false

  /**
    * 列信息
    */
  var columns: List[ColumnInfo] = List()

  override def toString = s"MetaInfo($charset, $delimiter, $dataType, $columns)"
}

class ColumnInfo {
  /**
    * 列名
    */
  var columnName = ""

  /**
    * 是否可为空
    */
  var columnNullable = true

  /**
    * 类型
    */
  var columnType = ""

  /**
    * 源文件中索引位置
    */
  var columnIndex = 0

  override def toString = s"ColumnInfo($columnName, $columnNullable, $columnType, $columnIndex)"
}
