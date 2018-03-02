package com.moon.utils

import org.apache.spark.sql.DataFrame

object DataFrameUtil {

  /**
    * 重新分区
    * 当 coalesce && repartition 均有设置时，智能匹配分区方案
    * 大于当前分区数，则使用 repartition
    * 其他情形使用 coalesce.
    *
    * @param df
    * @param numPartitions
    * @return
    */
  def autoRepartition(df: DataFrame, numPartitions: Int) : DataFrame = {

    val totalPartitions = df.rdd.partitions.size

    if (totalPartitions < numPartitions) {
      df.repartition(numPartitions)
    } else {
      df.coalesce(numPartitions)
    }
  }
}
