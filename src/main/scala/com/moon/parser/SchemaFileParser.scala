package com.moon.parser

import com.moon.models.{ColumnInfo, SchemaInfo}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.xml.XML

class SchemaFileParser {

  /**
    * 解析 meta 文件
    * @param metaPath meta文件路径
    * @return MetaInfo
    */
  def xmlSchemaParser(metaPath: String): SchemaInfo = {
    val schema = XML.loadFile(metaPath)

    val table = new SchemaInfo()
    // 解析 table base info
    val tableInfo = schema \ "source"
    table.dataType = (tableInfo \ "@type").text
    table.charset = (tableInfo \ "@charset").text
    table.delimiter = (tableInfo \ "@delimiter").text
    table.isHeaderFirst = if ((tableInfo \ "@isHeaderFirst").text.isEmpty) false
                          else (tableInfo \ "@isHeaderFirst").text.trim.toLowerCase() == "true"

    // 解析 columns
    val columnsInfo = tableInfo \ "columns"
    (columnsInfo \ "column").foreach(col => {
      val colAttr = new ColumnInfo()
      colAttr.columnName = col.text
      colAttr.columnType = (col \ "@type").text
      val nullable = (col \ "@nullable")
      if (nullable != null && nullable.text.toLowerCase == "false") {
        colAttr.columnNullable = false
      }
      colAttr.columnIndex = (col \ "@index").text.toInt
      table.columns = colAttr :: table.columns
    })

    table
  }
}

object SchemaFileParser {
  def main(args: Array[String]): Unit = {
    val schemaPath="/Users/mars/Desktop/dajiatong/daja3.0/20170515/0-data/meta/20170414/AL_CF_IDX_CASEFOLDER.meta.xml"
    val table = metaParser(schemaPath)
    println(table.charset, table.dataType, table.delimiter)
    table.columns.foreach(println)
  }

  /**
    * 解析 meta 文件
    * @param metaPath meta文件路径
    * @param mode 解析文件类型
    * @return MetaInfo
    */
  def metaParser(metaPath: String, mode: String = "xml"): SchemaInfo = {
    val schemaParser = new SchemaFileParser
    if (mode == "xml") schemaParser.xmlSchemaParser(metaPath)
    else new SchemaInfo
  }

  /**
    * 根据meta文件生成schema
    * @param path 文件路径
    * @return Schema
    */
  def generateSchemaFromMeta(path: String): (StructType, SchemaInfo) = {
    // get meta info
    val schemaInfo = SchemaFileParser.metaParser(metaPath = path, mode = "xml")
    // create schema field.
    val schema = schemaInfo.columns.sortBy(col => col.columnIndex).map(
      col => {
        StructField(col.columnName, StructTypeParser.getStructType(col.columnType), col.columnNullable)
      })

    (StructType(schema), schemaInfo)
  }

  /**
    * 根据meta文件生成schema
    * @param schemaInfo
    * @return Schema
    */
  def generateSchemaFromMeta(schemaInfo: SchemaInfo): StructType = {
    // create schema field.
    val schema = schemaInfo.columns.sortBy(col => col.columnIndex).map(
      col => {
        StructField(col.columnName, StructTypeParser.getStructType(col.columnType), col.columnNullable)
      })

    StructType(schema)
  }
}