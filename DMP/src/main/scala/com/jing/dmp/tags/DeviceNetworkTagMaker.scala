package com.jing.dmp.tags

import org.apache.spark.sql.Row

/**
  * 设备网络类型标签生成
  */
object DeviceNetworkTagMaker extends TagMaker{

	override def tagPrefix = "DN@"

	override def make(row: Row, dic: Map[String, String]) = {
		val network = row.getAs[String]("networkmannername")
		// 从字典中获取数据
		val networkId = dic.getOrElse(network, "D00020005")
		// 返回
		Map(s"$tagPrefix$networkId" -> 1.0)
	}
}
