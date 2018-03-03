package com.moon.parser

import com.moon.models.{Chain, ChainsInfo}

import scala.xml.XML

class ChainsFileParser {

  /**
    * 解析 transform 文件，xml格式
    * @param path
    * @return TransformInfo
    */
  def xmlChainsParser(path: String) : ChainsInfo = {
    val xml = XML.loadFile(path)

    val chainsInfo = new ChainsInfo

    // 解析 user-conf
    val default_conf = (xml \ "default-conf").text.trim
    val confXML = xml \ "user-conf" \ "conf"
    var user_conf: Map[String, String] = Map()
    confXML.map(s => {
      val name = (s \ "@name").text.trim
      val path = s.text.trim
      user_conf += (name -> path)
    })

    // 解析 base conf
    val baseConf = xml \ "base"
    chainsInfo.baseConf.tempPath = (baseConf \ "tempPath").text.trim
    chainsInfo.baseConf.logPath = (baseConf \ "logPath").text.trim
    chainsInfo.baseConf.schemaPath = (baseConf \ "schemaPath").text.trim
    chainsInfo.baseConf.metaPath = (baseConf \ "metaPath").text.trim

    // 解析 Chain
    var index = 0
    val chainConf = xml \ "chain"
    chainConf.foreach(s => {
      index += 1
      val chain = new Chain
      chain.transform = s.text.trim
      chain.run = (s \ "@run").text.trim
      chain.log = (s \ "@log").text.trim
      var conf = (s \ "@conf").text.trim
      if (conf.length > 0) {
        conf = user_conf.get(conf).getOrElse(default_conf)
      }
      chain.user_conf = conf
      chain.index = index
      chainsInfo.chains = chain :: chainsInfo.chains
    })

    chainsInfo
  }
}

object ChainsFileParser {
//  def main(args: Array[String]): Unit = {
//    var path = ".../resources/resources/chains.xml"
//    val chains = chainsParser(path)
//  }

  /**
    * 解析 Chains 文件，xml格式
    * @param path 文件路劲
    * @param mode 解析文件格式
    * @return List[Chain]
    */
  def chainsParser(path: String, mode: String = "xml"): ChainsInfo = {
    val chainsFileParser = new ChainsFileParser
    if (mode == "xml") chainsFileParser.xmlChainsParser(path)
    else null
  }
}