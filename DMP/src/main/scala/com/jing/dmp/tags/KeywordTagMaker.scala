package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 关键词标签(KeywordTag)生成
  */
object KeywordTagMaker extends TagMaker {

	override def tagPrefix: String = "KW@"

	override def make(row: Row, dic: Map[String, String]) = {
		row.getAs[String]("keywords")
			.split(",")
			.filter(word => StringUtils.isNotBlank(word))
			.map(word => (s"$tagPrefix$word", 1.0))
			.toMap
	}
}
