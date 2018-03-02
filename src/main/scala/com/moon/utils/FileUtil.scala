package com.moon.utils

import java.io.File

import scala.reflect.io.Path

object FileUtil {

  /**
    * 创建目录
    * @param path
    * @return
    */
  def createDirectoryIfNotExist(path: String) : Boolean = {
    val dir = new File(path)
    if (!dir.exists()) {
      return dir.mkdir()
    }

    true
  }

  /**
    * 创建目录
    * @param dir
    * @return
    */
  def createDirectoryIfNotExist(dir: File) : Boolean = {

    var success = true

    if (!dir.exists()) {
      success = dir.mkdirs()
    }

    success
  }

  /**
    * 创建文件
    * @param path
    * @return
    */
  def createFileIfNotExist(path: String) : Boolean = {
    val file = new File(path)
    if (!file.exists()) {
      return file.createNewFile()
    }

    true
  }

  /**
    * 创建文件
    * @param file
    * @return
    */
  def createFileIfNotExist(file: File) : Boolean = {
    if (!file.exists()) {
      return file.createNewFile()
    }

    true
  }
}
