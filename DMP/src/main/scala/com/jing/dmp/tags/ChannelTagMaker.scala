package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 渠道标签(ChannelTag)生成
  */
object ChannelTagMaker extends TagMaker {

  override def tagPrefix: String = "CH@"

  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {
    val channelid = row.getAs[String]("channelid")

    if (StringUtils.isNotBlank(channelid)) {
      Map(s"$tagPrefix$channelid" -> 1.0)
    } else {
      Map()
    }
  }
}
