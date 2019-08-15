package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * App名称标签(AppNameTag)生成
  */
object AppNameTagMaker extends TagMaker{

	override def tagPrefix = "APP@"

	override def make(row: Row, dic: Map[String, String]) = {

		// 获取应用ID信息
		val appId = row.getAs[String]("appid")

		val appName = dic.getOrElse(appId, "")
		if (StringUtils.isNotBlank(appName)) {
			Map(s"$tagPrefix$appName" -> 1.0)
		}
		else {
			Map()
		}
	}
}
