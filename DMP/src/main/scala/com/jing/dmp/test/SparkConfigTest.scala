package com.jing.dmp.test

import java.util

import com.typesafe.config.{ConfigFactory, ConfigValue}

object SparkConfigTest {

	def main(args: Array[String]): Unit = {

		// a.加载指定名称属性配置文件
		val sparkConfig = ConfigFactory.load("spark.conf")

		// b. 获取所有的配置信息
		val entrySet: util.Set[util.Map.Entry[String, ConfigValue]] = sparkConfig.entrySet()

		// c. 遍历Set集合中数据
		import scala.collection.JavaConverters._
		for(entry <- entrySet.asScala){
			val resource = entry.getValue.origin().resource()
			if("spark.conf".equals(resource)){
				val key = entry.getKey
				val value = entry.getValue.unwrapped().toString
				println(s"resource:$resource, $key = $value")
			}
		}
	}

}
