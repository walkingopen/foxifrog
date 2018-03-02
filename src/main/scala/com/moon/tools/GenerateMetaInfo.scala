package com.moon.tools

import java.io.{File, PrintWriter}

import com.moon.utils.FileUtil
import org.apache.spark.sql.types.StructType

import scala.io.Source
import scala.xml.XML

object GenerateMetaInfo {

  /**
    * generate meta file from the sql file
    */
  def generateMetaOfSql(): Unit = {
    val dir = "/Users/mars/Desktop/DP_SQL/sql"
    for (file <- new File(dir).listFiles().filter(!_.getName.contains(".DS_"))) {
      if (!file.getName.startsWith(".")) {
        var sql = Source.fromFile(file, enc = "UTF-8").mkString.replace("\n"," ").toUpperCase()
        sql = sql.replace("SELECT","") + "  "
        val cols = sql.split(" FROM ")(0).replace(" ","")
        var tableName = sql.split(" FROM ")(1).trim.split(" ")(0).trim
        tableName = tableName.replace("AMML.", "").replace("BAK_", "")
        print(tableName + "\n")
        // get columns
        var i = 0
        val columnElems = cols.split(",").map(col => {
          i += 1
          <column type="STRING" nullable="true" index={i.toString}> {col} </column>
        })

        //      println(cols)
        //      println(tableName)
        val meta =
        <Meta>
          <source table_name={tableName} type="xsv">
            <setting path="{{hdfs_path}}" charset="UTF-8" dateformat="yyyy-MM-dd HH:mm:ss" delimiter="\s\|\|\s" mode="DROPMALFORMED"></setting>
            <columns field_num={i.toString}>
              {columnElems}
            </columns>
          </source>
        </Meta>
        val head = """<?xml version="1.0" encoding="UTF-8"?>"""
        val pp = new xml.PrettyPrinter(80, 4)  // 行宽 80，缩进为 4
        val metaFile = pp.formatNodes(meta)
        //      println(head + "\n" + metaFile.toString)
        val savePath = "/Users/mars/Desktop/打假通/DP_SQL/meta_dp/"
        //      XML.save(savePath + tableName + ".meta.xml", meta, enc = "utf-8")
        val writer = new PrintWriter(new File(savePath + tableName + ".meta.xml" ))
        writer.write("""<?xml version="1.0" encoding="UTF-8"?>""")
        writer.write("\n")
        writer.write(metaFile)
        writer.close()
      }
    }
  }

  /**
    * generate sql file from the meta file
    */
  def generateSqlOfMeta(): Unit = {
    val dir = "/Users/mars/Desktop/DP_SQL/meta"
    for (file <- new File(dir).listFiles().filter(!_.getName.contains(".DS_")).filter(!_.getName().contains("DIM_CODE"))) {
      val meta = XML.loadFile(file)
      var tableName = (meta \ "source" \ "@table_name").text
      val field_num = (meta \ "source" \ "columns" \ "@field_num").text.toInt
      val cols = (meta \ "source" \ "columns" \ "column").map(col => {
        ((col \ "@index").text.toInt, col.text.trim.toUpperCase())
      }).filter(_._1 > 4).sortBy(_._1)

      //
      var tableNameEx = tableName
      if (tableName == "T_EMPLOYEE_VIEW") {
        tableName = "AL_BAK_" + tableName
        tableNameEx = "ex_al_" + tableNameEx.toLowerCase()
      } else {
        tableName = "AL_BAK_" + tableName.split("AL_")(1)
        tableNameEx = "ex_" + tableNameEx.toLowerCase()
      }
//      println(tableName)
      val savePath = "/Users/mars/Desktop/DP_SQL/"
      val writer = new PrintWriter(new File(savePath + "sql/" + tableName + ".sql" ))
      val writer1 = new PrintWriter(new File(savePath + "ex/" + tableNameEx + ".sql" ))
      writer.write("SELECT\n")
      writer1.write("SELECT\n")
      cols.foreach(col => {
        writer.write("    " + col._2)
        writer1.write("    " + col._2)
        if (field_num != col._1) {
          writer.write(",\n")
          writer1.write(",\n")
        }
      })
      writer.write(s"\nFROM AMML.$tableName")
      writer1.write(s"\nFROM ${tableNameEx}_current")
      writer1.write("\n  LIMIT 2")
      writer.close()
      writer1.close()
    }
  }

  /**
    * 根据注册表结构生成spark-sql建表语句文件
    * @param schema 字段名称
    * @param dbName 数据库名称，默认名：dbName.tableName
    * @param tableName 表名：ex_{tableName}
    * @param delimiter 数据分隔符
    * @param dataPath 数据存储路径
    * @param metaPath 生成的元数据保存路劲
    */
  def generateMetaSQLOfText(schema: StructType, dbName: String, tableName: String, delimiter: String, dataPath: String, metaPath: String) = {

    val exTable = s"${dbName}.ex_${tableName}_text"
    // create table statement
    val metaDir = new File(metaPath)
    FileUtil.createDirectoryIfNotExist(metaDir)
    val writer = new PrintWriter(new File(metaDir + "/" + exTable + ".sql"))
    writer.write(s"CREATE EXTERNAL TABLE ${exTable} (\n")

    // columns statement
    var index = 1
    schema.foreach(field => {
      val nullString = if (field.nullable) "" else " NOT NULL"
      var op_ch = ","
      if (index == schema.length) op_ch = ""
      writer.write(s"    ${field.name} ${field.dataType.sql}${nullString}$op_ch\n")
      index += 1
    })
    writer.write(")\n")

    // delimiter format statement
    if (delimiter.length == 1) {
      writer.write(s"ROW FORMAT DELIMITED FIELDS TERMINATED BY '$delimiter'\n")
    }
    else if (delimiter.length > 1) {
      writer.write("ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'\n")
      writer.write(s"""WITH SERDEPROPERTIES ("field.delim"="$delimiter")\n""")
    }
    else {
      throw new IllegalArgumentException(s"行分割符 valid！（行分割符：$delimiter）")
    }

    // location statement
    writer.write(s"LOCATION '$dataPath';")

    // close
    writer.close()
  }

  /**
    * 根据注册表结构生成spark-sql建表语句文件
    * @param dbName 数据库名称，默认名：dbName.tableName
    * @param tableName 表名：ex_{tableName}
    * @param dataPath 数据存储路径
    * @param metaPath 生成的元数据保存路劲
    */
  def generateMetaSQLOfParquet(dbName: String, tableName: String, dataPath: String, metaPath: String) = {

    val exTable = s"${dbName}.ex_${tableName}_parquet"
    // create table statement
    val metaDir = new File(metaPath)
    FileUtil.createDirectoryIfNotExist(metaDir)
    val writer = new PrintWriter(new File(metaDir + "/" + exTable + ".sql"))
    writer.write(s"CREATE EXTERNAL TABLE ${exTable}\n")
    writer.write(s"USING org.apache.spark.sql.parquet\n")
    writer.write(s"OPTIONS (\n")
    writer.write(s"    path ${dataPath}\n")
    writer.write(s");")
    // close
    writer.close()
  }

  /**
    * 根据注册表结构生成spark-sql建表语句文件
    * @param dbName 数据库名称，默认名：dbName.tableName
    * @param tableName 表名：ex_{tableName}
    * @param partitionBy
    * @param dataPath 数据存储路径
    * @param metaPath 生成的元数据保存路劲
    */
  def generateMetaSQLOfParquet(dbName: String, tableName: String, partitionBy: String, dataPath: String, metaPath: String) = {

    val exTable = s"${dbName}.ex_${tableName}_parquet"
    // create table statement
    val metaDir = new File(metaPath)
    FileUtil.createDirectoryIfNotExist(metaDir)
    val writer = new PrintWriter(new File(metaDir + "/" + exTable + ".sql"))
    writer.write(s"CREATE EXTERNAL TABLE ${exTable}\n")
    writer.write(s"USING org.apache.spark.sql.parquet\n")
    writer.write(s"PARTITIONED BY (${partitionBy})")
    writer.write(s"OPTIONS (\n")
    writer.write(s"    path ${dataPath}\n")
    writer.write(");")
    // close
    writer.close()
  }
}
