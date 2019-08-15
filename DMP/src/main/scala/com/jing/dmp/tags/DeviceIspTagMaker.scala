package com.jing.dmp.tags

import org.apache.spark.sql.Row

/**
  * 网络运营商的标签生成
  */
object DeviceIspTagMaker extends TagMaker{

	override def tagPrefix = "DI@"

	override def make(row: Row, dic: Map[String, String]) = {
		val isp = row.getAs[String]("ispname")
		// 从字典中获取数据
		val ipsId = dic.getOrElse(isp, "D00030004")
		// 返回
		Map(s"$tagPrefix$ipsId" -> 1.0)
	}
}
