package com.moon.parser

import scala.xml.XML
import com.moon.models.{SinkConf, SourceConf, TransformConf, TransformInfo}
import com.moon.validate.ArgumentsCheck

class TransformFileParser {

  /**
    * 解析 transform 文件，xml格式
    * @param path
    * @return TransformInfo
    */
  def xmlTransformParser(path: String) : TransformInfo = {
    var transXML = XML.loadFile(path)

    // 替换自定义参数
    var tranString = transXML.toString()
    (transXML \ "parameters" \ "parameter").foreach(paramter => {
      val pName = (paramter \ "@name").text.trim
      val pValue = (paramter \ "@value").text.trim
      tranString = tranString.replace("${" + pName + "}", pValue)
    })
    //transXML = tranString.asInstanceOf[Elem]
    transXML = XML.loadString(tranString)

    val transform: TransformInfo = new TransformInfo
    // 解析 base conf
    val baseConf = transXML \ "base"
    val tempPath = (baseConf \ "tempPath").text.trim
    val logPath = (baseConf \ "logPath").text.trim
    val metaPath = (baseConf \ "metaPath").text.trim
    val schemaPath = (baseConf \ "schemaPath").text.trim
    val dbName = (baseConf \ "database").text.trim
    transform.pipe.tempPath = if (tempPath.isEmpty) "~/temp/" else {  if (!tempPath.endsWith("/")) tempPath + "/" else tempPath }
    transform.pipe.logPath = if (logPath.isEmpty) "~/log/" else logPath
    transform.pipe.metaPath = if (metaPath.isEmpty) "~/meta/" else metaPath
    transform.pipe.schemaPath = if (schemaPath.isEmpty) "~/meta/" else schemaPath
    transform.pipe.dbName = if (dbName.isEmpty) "FROG" else dbName

    // 解析 source conf
    var index = 0
    val sourceConf = transXML \ "sources" \ "source"
    sourceConf.foreach(s => {
      val source = new SourceConf
      source.tableName = (s \ "@tableName").text.trim
      source.dataType = (s \ "@type").text.trim
      source.dataPath = (s \ "@dataPath").text.trim
      source.metaPath = (s \ "@metaPath").text.trim
      source.print = if ((s \ "@print").text.isEmpty) 0 else (s \ "@print").text.trim.toInt
      val coalesce = if ((s \ "@coalesce").text.isEmpty) 0 else (s \ "@coalesce").text.trim.toInt
      val repartition = if ((s \ "@repartition").text.isEmpty) 0 else (s \ "@repartition").text.trim.toInt
      val partitions = if ((s \ "@partitions").text.isEmpty) 0 else (s \ "@partitions").text.trim.toInt
      if (coalesce > 0) {
        source.partitions = coalesce
        source.partitionType = "coalesce"
      } else if (repartition > 0) {
        source.partitions = repartition
        source.partitionType = "repartition"
      } else if (partitions > 0) {
        source.partitions = partitions
        source.partitionType = "partitions"
      }

      source.cache = if ((s \ "@cache").text.isEmpty || !(s \ "@cache").text.trim.toLowerCase.equals("true"))
        false else true
      source.persist = if ((s \ "@persist").text.isEmpty || !(s \ "@persist").text.trim.toLowerCase.equals("true"))
        false else true
      source.index = index
      index += 1
      transform.sources = source :: transform.sources
    })

    // 解析 transform conf
    index = 1000
    val transformConf = transXML \ "transforms" \ "transform"
    transformConf.foreach(t => {
      val trans = new TransformConf
      trans.tableName = (t \ "@tableName").text.trim
      trans.dataType = (t \ "@type").text.trim
      trans.persist = if ((t \ "@persist").text.isEmpty) false else (t \ "@persist").text.trim.toLowerCase() == "true"
      trans.print = if ((t \ "@print").text.isEmpty) 0 else (t \ "@print").text.trim.toInt
      val coalesce = if ((t \ "@coalesce").text.isEmpty) 0 else (t \ "@coalesce").text.trim.toInt
      val repartition = if ((t \ "@repartition").text.isEmpty) 0 else (t \ "@repartition").text.trim.toInt
      val partitions = if ((t \ "@partitions").text.isEmpty) 0 else (t \ "@partitions").text.trim.toInt
      if (coalesce > 0) {
        trans.partitions = coalesce
        trans.partitionType = "coalesce"
      } else if (repartition > 0) {
        trans.partitions = repartition
        trans.partitionType = "repartition"
      } else if (partitions > 0) {
        trans.partitions = partitions
        trans.partitionType = "partitions"
      }
      trans.cache = if ((t \ "@cache").text.isEmpty || !(t \ "@cache").text.trim.toLowerCase.equals("true"))
        false else true
      trans.persist = if ((t \ "@persist").text.isEmpty || !(t \ "@persist").text.trim.toLowerCase.equals("true"))
        false else true
      trans.save = if ((t \ "@save").text.isEmpty || !(t \ "@save").text.trim.toLowerCase.equals("true"))
        false else true
      trans.sparkSql = t.text
      trans.index = index
      index += 1
      transform.transforms = trans :: transform.transforms
    })

    // 解析 sink conf
    index = 10000
    val sinkConf = transXML \ "sinks" \ "sink"
    sinkConf.foreach(sink => {
      val sk = new SinkConf
      sk.tableName = (sink \ "@tableName").text.trim
      sk.dataType = if ((sink \ "@type").isEmpty) "parquet" else (sink \ "@type").text.trim
      sk.savePath = (sink \ "@savePath").text.trim
      sk.saveMode = (sink \ "@saveMode").text.trim
      sk.delimiter = if ((sink \ "@delimiter").isEmpty) "," else (sink \ "@delimiter").text
      sk.partitionBy = (sink \ "@partitionBy").text.trim
      val coalesce = if ((sink \ "@coalesce").text.isEmpty) 0 else (sink \ "@coalesce").text.trim.toInt
      val repartition = if ((sink \ "@repartition").text.isEmpty) 0 else (sink \ "@repartition").text.trim.toInt
      val partitions = if ((sink \ "@partitions").text.isEmpty) 0 else (sink \ "@partitions").text.trim.toInt
      if (coalesce > 0) {
        sk.partitions = coalesce
        sk.partitionType = "coalesce"
      } else if (repartition > 0) {
        sk.partitions = repartition
        sk.partitionType = "repartition"
      } else if (partitions > 0) {
        sk.partitions = partitions
        sk.partitionType = "partitions"
      }
      //sk.gzip = if ((sink \ "@gzip").text == "true") true else false
      sk.print = if ((sink \ "@print").text.isEmpty) 0 else (sink \ "@print").text.trim.toInt

      // 存储方式解析
      if (!sk.savePath.isEmpty && sk.print > 0) {
        sk.saveType = "save_and_print"
      } else if (sk.print > 0) {
        sk.saveType = "print_only"
      } else {
        sk.saveType = "save_only"
      }

      sk.index = index
      index += 1
      transform.sinks = sk :: transform.sinks
    })

    transform
  }
}

object TransformFileParser {

  def main(args: Array[String]): Unit = {

    val path = "/Users/mars/Documents/Workspace/Coding/IdeaProjects/Scala/SpiderSpark/src/main/resources/transform.xml"
    val transform = transformParser(path)
    println(transform.pipe)
    transform.sources.foreach(println)
    transform.transforms.foreach(println)
    transform.sinks.foreach(println)
  }

  /**
    * 解析 transform 文件，xml格式
    * @param path 文件路劲
    * @param mode 解析文件格式
    * @return TranorsformInfo
    */
  def transformParser(path: String, mode: String = "xml"): TransformInfo = {
    var result = true
    var info: TransformInfo = null
    val transfromParser = new TransformFileParser
    if (mode == "xml") {
      info = transfromParser.xmlTransformParser(path)
      result = parametersValid(info)
    }
    else {
      System.err.println(s"文件[${path}] 格式不正确，支持格式[xml]，请检查！")
      result = false
    }

    if (!result) {
      System.err.println(s"在解析文件[${path}] 时发现以上问题, 请先解决, 然后在重新尝试！")
      System.exit(1000)
    }

    info
  }

  def parametersValid(info: TransformInfo): Boolean = {
    ArgumentsCheck.configParamsChcek(info)
  }
}
