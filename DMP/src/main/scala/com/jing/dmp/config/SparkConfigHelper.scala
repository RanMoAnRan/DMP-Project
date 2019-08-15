package com.jing.dmp.config

import java.util

import com.typesafe.config.{ConfigFactory, ConfigValue}
import org.apache.spark.sql.SparkSession

/**
  * 读取Spark Application应用参数属性值，从spark.conf中读取，并设置到SparkConf中
  */
class SparkConfigHelper(builder: SparkSession.Builder) {

	// a.加载指定名称属性配置文件
	val sparkConfig = ConfigFactory.load("spark.conf")

	def loadSparkConfig():  SparkSession.Builder = {
		// b. 获取所有的配置信息
		val entrySet: util.Set[util.Map.Entry[String, ConfigValue]] = sparkConfig.entrySet()
		// c. 遍历Set集合中数据
		import scala.collection.JavaConverters._
		for(entry <- entrySet.asScala){
			val resource = entry.getValue.origin().resource()
			if("spark.conf".equals(resource)){
				val key = entry.getKey
				val value = entry.getValue.unwrapped().toString
				// 设置属性到SparkSession Buildder中
				builder.config(key, value)
			}
		}
		// 返回Builder对象
		builder
	}

}

object SparkConfigHelper{

	implicit def builder2Helper(builder: SparkSession.Builder): SparkConfigHelper = {
		new SparkConfigHelper(builder)
	}

}
