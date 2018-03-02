package com.moon.foxi.cache

import com.moon.models.{SinkConf, SourceConf, TransformConf, TransformInfo}

/// TODO: 如何确定在 此缓存表已经使用完成后，自动将其 uncache 掉，避免资源紧张或者浪费掉了
object CacheManage {

  // 规则：每个 `cache` 表最后一次使用的表时，进行 `uncache`
  def gemCacheDAG(sourceTables: List[SourceTB], transformTables: List[TransformTB])
    : List[CacheTable] = {

    // get all cached tables.
    val cacheTables = sourceTables.filter(_.isCache).map(t => (t.index, t.tableName)) ++
      transformTables.filter(_.isCache).map(t => (t.index, t.tabbleName))

    // get all the tigger of the uncache event.
    cacheTables.map(ct => {
      val lastT = transformTables.filter(t => t.index > ct._1 && t.relatedTables.contains(ct._2))
        .sortWith((x, y) => x.index > y.index)
      if (lastT.size == 0) {//Transform没有其他表引用此 cache ,返回自己
        CacheTable(ct._1, ct._2, ct._2)
      } else {
        CacheTable(lastT.head.index, lastT.head.tabbleName, ct._2)
      }
    })
  }

  // 获取所有表
  def getAllTables(sources: List[SourceConf], transforms: List[TransformConf], sinks: List[SinkConf]): AllTBS = {

    // sink tables
    val sinkTables = sinks.map(s => SinkTB(s.index, s.tableName,
      s.savePath.length > 0, s.print > 0)).sortBy(_.index)

    // source tables
    val sourceTables = sources.map(s => {
      val isSink = sinkTables.filter(_.tableName == s.tableName).count(_ => true) > 0
      SourceTB(s.index, s.tableName, s.cache, isSink)
    }).sortBy(_.index)

    // transform tables
    val tables = sources.map(_.tableName) ++ transforms.map(_.tableName)
    val transformTables = transforms.map(s => {
      val isSink = sinkTables.filter(_.tableName == s.tableName).count(_ => true) > 0
      // 处理依赖表
      val relatedTables = getRelatedTables(s.sparkSql, tables)
      TransformTB(s.index, s.tableName, s.cache, s.save, isSink, relatedTables)
    }).sortBy(_.index)

    // all tables
    AllTBS(sourceTables, transformTables, sinkTables)
  }

  // 处理 transform sql 依赖
  def getRelatedTables(sql: String, tables: List[String]): List[String] = {

    val sqlR = sql.replace("\n", " ")
    // 分离出每个transform实现关联的 table
    val relatedTables = tables.filter(name => sql.contains(" " + name + " "))
    relatedTables
  }

}

// 所有表合集
case class AllTBS(sourceTBS: List[SourceTB], transformTBS: List[TransformTB], sinkTBS: List[SinkTB])
// 源表
case class SourceTB(index: Int, tableName: String, isCache: Boolean, isSink: Boolean)
// 转换表
case class TransformTB(index: Int, tabbleName: String, isCache: Boolean,
                       isSave: Boolean, isSink: Boolean, relatedTables: List[String])
// 下沉表
case class SinkTB(index: Int, tableName: String, isSave: Boolean, isPrint: Boolean)
case class CacheTable(index: Int, tiggerTable: String, uncacheTable: String)