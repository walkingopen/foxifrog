package com.moon.foxi

object CacheType {

  val CACHE: String = "cache"
  val PERSIST: String = "persist"

}

object PartitionType {

  val COALESCE: String = "coalesce"
  val REPARTITION: String = "repartition"
  val PARTITION: String = "partitions"

}

object DataType {
  val PARQUET: String = "parquet"
  val CSV: String = "csv"
  val TXT: String = "txt"
  val JSON: String = "json"
  val ORC: String = "orc"
}

object SaveType {
  val PRINT_ONLY: String = "print_only"
  val SAVE_ONLY: String = "save_only"
  val SAVE_AND_PRINT: String = "save_and_print"
}

object TableType {
  val SOURCE: String = "source"
  val TRANSFORM: String = "transform"
  val SINK: String = "sink"
}