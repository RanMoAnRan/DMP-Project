package com.jing.dmp.tags

import org.apache.spark.sql.Row

/**
  * 性别标签(GenderTag)生成
  */
object GenderTagMaker extends TagMaker{

	override def tagPrefix = "GD@"

	override def make(row: Row, dic: Map[String, String]) = {
		// 获取性别信息
		val sex = row.getAs[String]("sex")
		// 计算并返回标签
		sex match {
			case "1" => Map(s"${tagPrefix}男" -> 1.0)
			case "0" => Map(s"${tagPrefix}女" -> 1.0)
			case _ => Map()
		}
	}
}