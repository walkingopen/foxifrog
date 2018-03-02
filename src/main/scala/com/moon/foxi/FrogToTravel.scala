package com.moon.foxi

import org.apache.spark.sql.SparkSession
import com.moon.models._
import com.moon.parser.SchemaFileParser
import com.moon.foxi.core.Frog
import com.moon.foxi.cache.{CacheManage, CacheTable}
import com.moon.tools.GenerateMetaInfo

class FrogToTravel(sparkSession: SparkSession) {

  var base: PipeConf = null
  var equipments: List[SourceConf] = null
  var scenerys: List[TransformConf] = null
  var photos: List[SinkConf] = null
  val frog = Frog(sparkSession = sparkSession)

  def onTheWay(): Unit = {

//    val base = info.pipe
//    val equipments = info.sources     // 准备
//    val scenerys = info.transforms    // 看风景
//    val photos = info.sinks           // 拍照

    val allTables = CacheManage.getAllTables(equipments, scenerys, photos)
    val cacheTables = CacheManage.gemCacheDAG(allTables.sourceTBS, allTables.transformTBS)

    equipments.sortBy(_.index).foreach(equipment => {
        // 1.注册Table
        this.register(equipment)

        // 2.print检查: 防止由print提前出发cache操作
        if (equipment.print > 0) {
          frog.print(sparkSession, equipment.tableName, equipment.print)
        }

        // 3.检查cache
        if (equipment.cache) {
          println(s"cache table: ${equipment.index}-${equipment.tableName}")
          frog.cacheTable(equipment.tableName)
        }

        // 4.判断是否为最终存储表
        photos.filter(_.tableName == equipment.tableName).sortBy(_.index).foreach(photo => {

          val saveType = photo.saveType
          if (saveType == SaveType.PRINT_ONLY) {
            frog.print(sparkSession, photo.tableName, photo.print)
          } else {
            // save
            this.save(photo)
            this.gemMetaInfoSQLFile(TableType.SINK, photo.tableName, photo.savePath, photo.dataType, photo.partitionBy, photo.delimiter)

            // print
            if (saveType == SaveType.SAVE_AND_PRINT) {
              frog.print(sparkSession, photo.tableName, photo.print)
            }
          }
        })

        // 5.cache管理
        this.uncacheTable(equipment.tableName, cacheTables)

      // 6.生成元数据信息文件
      var delimiter = ""
      if (List("txt", "csv").contains(equipment.dataType) && !equipment.metaPath.isEmpty) {
        delimiter = SchemaFileParser.generateSchemaFromMeta(equipment.metaPath)._2.delimiter
      }
      this.gemMetaInfoSQLFile(TableType.SOURCE, equipment.tableName, equipment.dataPath, equipment.dataType, "", delimiter)
    })

    // 看风景
    val tempPath = base.tempPath
    scenerys.sortBy(_.index).foreach(scenery => {

      // 1.转换
      val sinkTables = photos.filter(_.tableName == scenery.tableName)
      scenery.isSink = sinkTables.length > 0
      this.transform(scenery, tempPath)

      // 2.检查cache
      if (scenery.cache) {
        println(s"cache table: ${scenery.index}-${scenery.tableName}")
        frog.cacheTable(scenery.tableName)
      }

      // 3.判断是否为最终存储表
      sinkTables.sortBy(_.index).foreach(photo => {

        val saveType = photo.saveType
        if (saveType == SaveType.PRINT_ONLY) {
          frog.print(sparkSession, photo.tableName, photo.print)
        } else {
          // save
          this.save(photo)
          this.gemMetaInfoSQLFile(TableType.SINK, photo.tableName, photo.savePath, photo.dataType, photo.partitionBy, photo.delimiter)

          // print
          if (saveType == SaveType.SAVE_AND_PRINT) {
            frog.print(sparkSession, photo.tableName, photo.print)
          }
        }
      })

      // 4.cache管理
      this.uncacheTable(scenery.tableName, cacheTables)

      // 5.生成元数据信息文件
      this.gemMetaInfoSQLFile(TableType.TRANSFORM, scenery.tableName, base.tempPath, DataType.PARQUET, "", "")
    })

    // 拍照
    println("Take photos done. All the photos had already stored.")
  }

  // Table注册
  def register(equipment: SourceConf): Unit = {

    System.out.println("=" * 20 + s" register[${equipment.index}] starting " + "=" * 20)
    equipment.printInfo()

    equipment.dataType.toLowerCase() match {
      case DataType.PARQUET => {
        if (equipment.partitions > 0) {
          frog.createTable(equipment.tableName, equipment.dataPath,equipment.partitions, equipment.partitionType)
        } else {
          frog.createTable(equipment.tableName, equipment.dataPath)
        }
      }
      case DataType.CSV => {
        if (equipment.metaPath.isEmpty) {
          if (equipment.partitions > 0) {
            frog.createTableFromCSV(equipment.tableName, equipment.dataPath, equipment.partitions, equipment.partitionType)
          } else {
            frog.createTableFromCSV(equipment.tableName, equipment.dataPath)
          }
        } else {
          val schema = SchemaFileParser.generateSchemaFromMeta(equipment.metaPath)._1
          if (equipment.partitions > 0) {
            frog.createTableFromCSV(equipment.tableName, equipment.dataPath, schema, equipment.partitions, equipment.partitionType)
          } else {
            frog.createTableFromCSV(equipment.tableName, equipment.dataPath, schema)
          }
        }
      }
      case DataType.TXT => {
        val desc = SchemaFileParser.generateSchemaFromMeta(equipment.metaPath)
        if (equipment.partitions > 0) {
          frog.createTableFromTXT(equipment.tableName, equipment.dataPath,
            desc._1,
            desc._2.delimiter,
            equipment.partitions,
            equipment.partitionType,
            autoAdopt=true,
            charset = desc._2.charset)
        } else {
          frog.createTableFromTXT(equipment.tableName, equipment.dataPath, desc._1, desc._2.delimiter, autoAdopt=true, charset = desc._2.charset)
        }
      }
      case _ => {

      }
    }

    System.out.println("=" * 20 + s" register[${equipment.index}] ending " + "=" * 20)

  }

  // 逻辑转换
  def transform(scenery: TransformConf, tempPath: String): Unit = {

    System.out.println("=" * 20 + s" transform[${scenery.index}] starting " + "=" * 20)
    scenery.printInfo()

    val path = tempPath + scenery.tableName
    if (scenery.partitions > 0) {
      if (scenery.save && !scenery.isSink) {
        frog.transformTable(scenery.tableName, scenery.sparkSql, path, scenery.partitions, scenery.partitionType)
      } else {
        frog.transformTable(scenery.tableName, scenery.sparkSql, scenery.partitions, scenery.partitionType)
      }
    } else {
      if (scenery.save && !scenery.isSink) {
        frog.transformTable(scenery.tableName, scenery.sparkSql, path)
      } else {
        frog.transformTable(scenery.tableName, scenery.sparkSql)
      }
    }

    System.out.println("=" * 20 + s" transform[${scenery.index}] ending " + "=" * 20)

  }

  // 数据下沉
  def save(photo: SinkConf): Unit = {

    System.out.println("=" * 20 + s" sink[${photo.index}] starting " + "=" * 20)
    photo.printInfo()

    photo.dataType.toLowerCase() match {
      case DataType.PARQUET => {
        frog.saveTable(photo.tableName, photo.savePath, photo.partitionBy, photo.partitions, photo.partitionType, photo.saveMode)
      }
      case DataType.CSV => {
        frog.saveTableAsCSV(photo.tableName, photo.savePath, photo.partitionBy, photo.partitions, photo.partitionType, photo.saveMode)
      }
      case DataType.TXT => {
        frog.saveTableAsTXT(photo.tableName, photo.savePath, photo.partitionBy, photo.partitions, photo.partitionType, photo.delimiter, photo.saveMode)
      }
      case _ => {
        frog.saveTable(photo.tableName, photo.savePath, photo.partitionBy, photo.partitions, photo.partitionType, photo.saveMode)
      }
    }

    System.out.println("=" * 20 + s" sink[${photo.index}] ending " + "=" * 20)

  }

  // 触发cache释放管理
  def uncacheTable(tiggerTable: String, cacheTables: List[CacheTable]): Unit = {
    cacheTables.filter(_.tiggerTable == tiggerTable).foreach(ct => {
      println(s"Uncache table: ${ct.index}-${ct.uncacheTable}; tigger table: ${tiggerTable}")
      frog.uncacheTable(ct.uncacheTable)
    })
  }

  // 生成表的元数据信息
  def gemMetaInfoSQLFile(tableType: String ,table: String, dataPath: String, dataType: String, partitionBy: String, delimiter: String): Unit = {

    val dbName = base.dbName
    val metaPath = base.metaPath
    val schema = sparkSession.table(table).schema

    val tableName = tableType + "_" + table

    dataType.toLowerCase() match {
      case DataType.PARQUET => {
        if (partitionBy.isEmpty) {
          GenerateMetaInfo.generateMetaSQLOfParquet(dbName, tableName, dataPath, metaPath)
        } else {
          GenerateMetaInfo.generateMetaSQLOfParquet(dbName, tableName, partitionBy, dataPath, metaPath)
        }
      }
      case DataType.CSV | DataType.TXT => {
        GenerateMetaInfo.generateMetaSQLOfText(schema, dbName, tableName, delimiter, dataPath, metaPath)
      }
      case _ => {
      }
    }
  }

}

object FrogToTravel {

  def apply(sparkSession: SparkSession, info: TransformInfo): FrogToTravel = {
    val travel = new FrogToTravel(sparkSession)
    travel.base = info.pipe
    travel.equipments = info.sources
    travel.scenerys = info.transforms
    travel.photos = info.sinks

    travel
  }
  
}
