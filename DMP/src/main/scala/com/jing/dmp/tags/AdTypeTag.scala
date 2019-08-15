package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告类型(AdTypeTag)生成
  */
object AdTypeTag extends TagMaker {

  override def tagPrefix: String = "AD@"

  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    // a. 从Row中依据字段名称获取AdType值
    val adValue = row.getAs[Long]("adspacetype")
    // b. 将广告类型转换为名称 -> 广告位类型（1：banner 2：插屏 3：全屏）
    if (StringUtils.isNotEmpty(adValue.toString)) {
      adValue match {
        case 1 => Map(s"${tagPrefix}banner" -> 1.0)
        case 2 => Map(s"${tagPrefix}插屏" -> 1.0)
        case 3 => Map(s"${tagPrefix}全屏" -> 1.0)
      }
    } else {
      Map()
    }
  }
}
