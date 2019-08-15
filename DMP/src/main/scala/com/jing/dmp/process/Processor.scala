package com.jing.dmp.process

import org.apache.spark.sql.DataFrame

/**
  * 数据处理接口，其中函数processData对数据集DataFrame进行处理转换
  */
trait Processor {

  def processData(dataframe: DataFrame): DataFrame

}
