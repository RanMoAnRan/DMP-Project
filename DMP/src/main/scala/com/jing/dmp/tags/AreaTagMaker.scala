package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 商圈标签(AreaTag)生成
  */
object AreaTagMaker extends TagMaker {

	override def tagPrefix = "BA@"

	override def make(row: Row, dic: Map[String, String]) = {
		val areas: String = row.getAs[String]("area")

		areas.split(":")
			.filter(word => StringUtils.isNotBlank(word))
			.map(word => (s"$tagPrefix$word", 1.0))
			.toMap
	}
}
