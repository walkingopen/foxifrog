package com.moon.demo

object requireDemo {

  def main(args: Array[String]): Unit = {

    // 正常执行
    require(true)

    // 报错, 返回值 1
    require(false)
    require(false, "好像缺少了什么？")

  }

}
