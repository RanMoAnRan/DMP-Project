package com.jing.dmp.tags

import org.apache.spark.sql.Row

/**
  * 依据数据生成各个标签的统一接口Trait
  */
trait TagMaker {

  // 标签的前缀
  def tagPrefix: String

  // 每行数据Row，产生对应的标签
  def make(row: Row, dic: Map[String, String] = null): Map[String, Double]

}
