package com.moon.demo

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.hipi.image.{ByteImage, FloatImage, HipiImage, HipiImageHeader}
import org.hipi.imagebundle.mapreduce.HibInputFormat

object HibToTable {
  def main(args: Array[String]): Unit = {
    val dataPath = "file:///Users/mars/Documents/Kitematic/hadoop/hipi/data/*.hib"

    val conf = new SparkConf().setAppName("fdsfs").setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val input =
    //sc.newAPIHadoopFile[HipiImageHeader, HipiImage, HibInputFormat](dataPath)
    val rdd = sc.newAPIHadoopFile(dataPath,
      classOf[HibInputFormat],
      classOf[HipiImageHeader],
      classOf[HipiImage],
      new Configuration(sc.hadoopConfiguration)
    )
    val hadoopRdd = rdd.asInstanceOf[NewHadoopRDD[HipiImageHeader, ByteImage]]
    hadoopRdd.map(img => (img._1.getAllMetaData, img._2.getData)).first()
//    ({md5=13a0efed82219e4334835cf73e18062f, source=/home/data/Images/test/1.jpeg, filename=1.jpeg},
//      Array(-121, -124, -107, -113, -117, -100, -107, -118, -101, -113, -126, -108, -117, 126, -113,
//        -117, -125, -110, -119, -121, -110, -121, -121, -111, 124, -126, -114, 127, -120, -105, -128,
//        -112, -99, 120, -116, -117, 55, 76, 59, 40, 52, 32, 40, 40, 28, 46, 37, 38, 41, 44, 35, 38, 46,
//        33, 43, 49, 37, 49, 39, 29, 67, 30, 21, -101, 97, 86, -61, -127, 113, -53, -118, 116, -15, -94,
//        -89, -18, -62, -65, -3, -1, -12, -29, -1, -11, -21, -1, -1, -16, -6, -5, -1, -10, -5, -1, -7,
//        -1, -3, -3, -1, -7, -1, -3, -7, -1, -15, -1, -1, -22, -1, -4, -24, -1, -6, -14, -1, -5, -1,
//        -4, -7, -1, -2, -2, -10, -15, -25, -37, -10, -36, -53, -72, -108, 124, -72, -11...)
  }
}
