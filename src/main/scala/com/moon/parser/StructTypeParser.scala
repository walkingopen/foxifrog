package com.moon.parser

import org.apache.spark.sql.types._

/**
  * define the Schema StructType.
  */
object StructTypeParser {
  /**
    * Schema StructType Mapping
    * @param typename
    * @return
    */
  def getStructType(typename: String): DataType = {
    typename.toLowerCase match {
      case "int" => IntegerType
      case "integer" => IntegerType
      case "string" => StringType
      case "long" => LongType
      case "double" => DoubleType
    }
  }
}