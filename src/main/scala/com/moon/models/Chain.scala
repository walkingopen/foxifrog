package com.moon.models

class ChainsInfo {

  /**
    * 基础信息
    */
  var baseConf: BaseConf = new BaseConf

  /**
    * 流程
    */
  var chains: List[Chain] = List()
}

class Chain {
  var log: String = ""
  var run: String = "error_stop"
  var transform: String = ""
  var user_conf: String = ""
  var index = 0

  override def toString = s"Chain($log, $run, $transform)"
}

class BaseConf {
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
    * 元数据存放文件夹
    * LOCAL
    */
  var metaPath = ""

  /**
    * Mapper存放文件夹
    * LOCAL
    */
  var schemaPath = ""
}