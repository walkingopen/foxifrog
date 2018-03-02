package com.moon.demo

import scala.util.Random

object example {

  def main(args: Array[String]): Unit = {
//    val s = setValue("")
//    println(s.isEmpty)

    stripMargin()

  }

  def setValue(s: String): String = {
    s
  }


  def stripMargin(): Unit = {
    val schemaString =
      s"""{
        |   "namespace": "example.avro",
        |   "type": "record",
        |   "name": "User",
        |   "fields": [
        |        {"name": "name", "type": "string"},
        |        {"name": "favorite_number",  "type": ["int", "null"]},
        |        {"name": "favorite_color", "type": ["string", "null"]}
        |    ]
        |}""".stripMargin

    println(schemaString)
  }

}
