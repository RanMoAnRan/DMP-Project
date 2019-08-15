package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 城市标签(CityTag)生成
  */
object CityTagMaker extends TagMaker{

	override def tagPrefix = "CT@"

	override def make(row: Row, dic: Map[String, String]) = {
		val city = row.getAs[String]("city")
		if (StringUtils.isNotBlank(city)) {
			Map(s"$tagPrefix$city" -> 1.0)
		}
		else {
			Map[String, Double]()
		}
	}
}
