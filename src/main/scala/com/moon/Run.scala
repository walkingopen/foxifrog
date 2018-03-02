package com.moon

import com.moon.foxi.FrogToTravel
import com.moon.parser.ChainsFileParser.chainsParser
import com.moon.parser.TransformFileParser
import com.moon.spark.SparkDemo
import org.apache.log4j.{Level, Logger}

object Run {
  def main(args: Array[String]): Unit = {
    val spark = SparkDemo.getContext()
    // logs
    Logger.getLogger(getClass.getName).setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.WARN)

//    val data = spark.read.parquet("/Users/mars/Desktop/Data/bank/bank/people_out")
//    data.printSchema()
//    data.foreach(p => println(p))
//    println(data.count())
//    val a = 111
    // 获取全部任务
    val basePath = "/Users/mars/Documents/Workspace/Coding/IdeaProjects/Scala/SpiderSpark/src/main/resources/"
    val path = basePath + "chains.xml"
    val chainsInfo = chainsParser(path)
    // println(chains.length)
    chainsInfo.chains.foreach(c => {
      // 清空背内部缓存临时表
      //spark.catalog.clearCache()
      // get config
      val chain = basePath + c.transform
      val config = TransformFileParser.transformParser(chain)
      val frog = FrogToTravel(spark, info = config)
      frog.onTheWay()
    })
  }
}
