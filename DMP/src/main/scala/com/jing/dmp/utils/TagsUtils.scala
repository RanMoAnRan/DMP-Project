package com.jing.dmp.utils

import scala.collection.mutable

/**
  * 将Map集合转换为字符串工具类
  */
object TagsUtils {

	/**
	  * Map类型的数据转换为字符串，格式为：(k1->v1),(k2->v2),(k3->v3)
	  *
	  * @param kvMap Map 集合
	  * @return
	  */
	def map2Str(kvMap: Map[String, _]): String = {
		kvMap
			.toList.sortBy(_._1) // 转换为List集合，按照Key排序
			.map{case (key, value) => s"($key->$value)" }.mkString(",")
	}

	/**
	  * 从Kudu中获取到的标签信息是字符串类型转换为Map集合
	  *
	  * @param tagsStr 标签数据字符串
	  * @return
	  */
	def tagsStr2Map(tagsStr: String): Map[String, Double] = {
		// 数据格式：(AD@banner->1.0),(AGE@35->1.0),(APP@蝉大师->1.0)
		// a. 按照逗号分割
		val tags: Array[String] = tagsStr.split("\\,")

		var map: mutable.Map[String, Double] = mutable.Map[String, Double]()
		// b. 遍历数组中元素处理后存入Map集合
		for(tagStr <- tags){
			// 去除字符串前后左右括号，以->分割
			val Array(tagKey, tagValue) = tagStr
				.stripPrefix("(").stripSuffix(")").split("->")
			map += tagKey -> tagValue.toDouble
		}

		// c. 返回不可变集合
		map.toMap
	}

	/**
	  * 从Kudu中获取到的id集合是字符串类型转换为Map
	  *
	  * @param idsStr 用户标识IDs字符串
	  *
	  * @return
	  */
	def idsStr2Map(idsStr: String): Map[String, String] = {
		// 数据格式：(imei->214052893329662),(mac->52:54:00:50:12:35)
		// a. 按照逗号分割
		val ids: Array[String] = idsStr.split("\\,")

		var map: collection.Map[String, String] = collection.Map[String, String]()
		// b. 遍历数组中元素处理后存入Map集合
		for(idStr <- ids){
			// 去除字符串前后左右括号，以->分割
			val Array(idKey, idValue) = idStr
				.stripPrefix("(").stripSuffix(")").split("->")
			map += idKey -> idValue
		}

		// c. 返回不可变集合
		map.toMap
	}


}
