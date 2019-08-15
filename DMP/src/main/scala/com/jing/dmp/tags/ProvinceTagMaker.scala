package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 省份标签(ProvinceTag)生成
  */
object ProvinceTagMaker extends TagMaker {

  override def tagPrefix: String = "PN@"

  override def make(row: Row, dic: Map[String, String]) = {
    val province = row.getAs[String]("province")

    if (StringUtils.isNotBlank(province)) {
      Map(s"$tagPrefix$province" -> 1.0)
    }
    else {
      Map[String, Double]()
    }
  }
}