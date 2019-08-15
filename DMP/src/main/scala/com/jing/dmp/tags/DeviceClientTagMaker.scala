package com.jing.dmp.tags

import org.apache.spark.sql.Row

/**
  * 设备类型标签的生成
  */
object DeviceClientTagMaker extends TagMaker {

	override def tagPrefix = "DC@"

	override def make(row: Row, dic: Map[String, String]) = {
		val client = row.getAs[Long]("client").toString
		// 从字典中获取数据
		val clientId = dic.getOrElse(client, "D00010004")
		// 返回
		Map(s"$tagPrefix$clientId" -> 1.0)
	}
}
