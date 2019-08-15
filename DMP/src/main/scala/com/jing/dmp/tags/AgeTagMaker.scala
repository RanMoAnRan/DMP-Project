package com.jing.dmp.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 年龄标签(AgeTag)生成
  */
object AgeTagMaker extends TagMaker{

	override def tagPrefix = "AGE@"

	override def make(row: Row, dic: Map[String, String]) = {
		val age = row.getAs[String]("age")

		if (StringUtils.isNotBlank(age)){
			Map(s"$tagPrefix$age" -> 1.0)
		} else{
			Map[String, Double]()
		}
	}
}
