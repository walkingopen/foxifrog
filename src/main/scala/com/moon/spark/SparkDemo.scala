package com.moon.spark

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

class SparkDemo {

  /**
    * 程序结果保存路径
    */
  var resultPath:String = ""

  /**
    * 新建一个SparkContext
    * 适配Windows和Linux开发环境
    * @param systemType Windows或Linux
    * @return SparkContext
    */
  def createSparkContext(systemType: String, configFile: String): SparkSession = {
    var appName: String = this.getClass.getSimpleName
    var sparkHome: String = ""
    var cleanCommand: String = ""

    if (systemType != SystemType.WINDOWS) {
      // 非Windows系统
      resultPath = "/Users/mars/Documents/Workspace/Coding/IdeaProjects/Scala/SpiderSpark/out/data/" + appName
      sparkHome = "/Users/mars/Documents/Workspace/SDK/hadoop/SOURCE/spark-2.0.0-bin-hadoop2.7"
      cleanCommand = "rm -rf " + resultPath
    } else {
      // Winddows系统
      resultPath = "..."
      sparkHome = "..."
      cleanCommand = "cmd.exe /c rd /s/q " + resultPath
    }

    // 清理之前生成的输出目录
    println(cleanCommand)
    Runtime.getRuntime.exec(cleanCommand)

    // 创建SparkContext
    SparkSession.clearDefaultSession()
    var master = "local[*]"
    appName = "NO SET"
    var properties: Map[String, String] = Map()
    if (!configFile.isEmpty) {
      properties = getSparkProperties(configFile)
      if (properties.contains("spark.master") && !properties.get("spark.master").isEmpty) {
        master = properties.get("spark.master").getOrElse(master)
      }
      if (properties.contains("spark.app.name") && !properties.get("spark.app.name").isEmpty) {
        appName = properties.get("spark.app.name").getOrElse(appName)
      }
    }

    val sparkSession = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
    // 自定义配置加载
    properties.foreach(pro => {
      sparkSession.conf.set(pro._1, pro._2)
    })
    SparkSession.clearDefaultSession()
    sparkSession
  }

  /**
    * 加载用户自定义配置文件参数
    * @param file
    * @return
    */
  def getSparkProperties(file: String): Map[String, String] = {
    var sparkProperties: Map[String, String] = Map()
    val properties = new Properties()
    properties.load(new FileInputStream(file))
    properties.keySet().toArray.foreach(key => {
      sparkProperties += (key.toString -> properties.get(key).toString)
    })
    sparkProperties
  }
}

object SparkDemo {

  /**
    * 获取 SparkContext
    * @param systemType 系统类别：WINDOWS;MACOS;LINUX
    */
  def getContext(systemType: String = SystemType.MACOS, configFile: String = ""): SparkSession = {
    val sd = new SparkDemo()
    sd.createSparkContext(systemType, configFile)
  }
}
