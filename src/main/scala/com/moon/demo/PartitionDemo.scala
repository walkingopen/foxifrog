package com.moon.demo

import com.moon.spark.SparkDemo
import org.apache.spark.sql.{DataFrame, SaveMode}

case class People(age: Int, job: String, education: String, day: Int)

object PartitionDemo {
  val spark = SparkDemo.getContext()

  def main(args: Array[String]): Unit = {
    // create demo data
    createParquet()

    // read demo data
    val df = readParquet()
    df.createOrReplaceTempView("people")
//    spark.sql("select age, job, education, day from people").collect().foreach(println)
    spark.sql("select age, job, education, day from people where education='primary'").collect().foreach(println)
//    println(df.count())
    val paruetPath =  "/Users/mars/Desktop/Data/bank/bank/"
//    spark.sql("select age, job, education, day from people where education='primary'")
//      .write.mode(SaveMode.Append).parquet(paruetPath + "people/education=primary")
  }

  def createParquet(): Unit = {
    val data = "/Users/mars/Desktop/Data/bank/bank.csv"
    val rdd = spark.sparkContext.textFile(data).filter(!_.contains("age"))
      .map(_.replace("\"","").split(";"))
      .map(cols => People(cols(0).toInt, cols(1), cols(3), cols(9).toInt))
    val df = spark.createDataFrame(rdd)
    df.printSchema()
    df.createOrReplaceTempView("people")
    val paruetPath =  "/Users/mars/Desktop/Data/bank/bank/"
    //    spark.sql("select age, job, education, day from people").take(100).foreach(println)
    spark.sql("select age, job, education, day from people limit 10")
      .write.partitionBy("education").mode(SaveMode.Overwrite).parquet(paruetPath + "people")
  }

  def readParquet(): DataFrame = {
    val path = "/Users/mars/Desktop/Data/bank/bank/people"
    val df = spark.read.parquet(path).toDF()
//    df.printSchema()
    df
  }
}
