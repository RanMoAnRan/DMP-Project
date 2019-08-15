package com.jing.dmp.utils

import com.jing.dmp.config.AppConfigHelper
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * 构建SparkSession会话实例对象工具类
  */
object SparkSessionUtils extends Logging{

	def createSparkSession(clazz: Class[_]): SparkSession = {
		// 1、Spark Application相关参数设置
		val sparkConf: SparkConf = {
			val conf = new SparkConf()
				.setAppName(clazz.getSimpleName.stripSuffix("$"))
			// TODO: 判断是否是本地模式，如果是本地模式，设置值，否则不设置，通过提交应用--master设置
			if(AppConfigHelper.SPARK_APP_LOCAL_MODE.toBoolean){
				logInfo("Spark Application运行的本地模式..................")
				conf.setMaster(AppConfigHelper.SPARK_APP_MASTER)
			}
			conf
		}

		// 2、构建SparkSession实例对象，传递应用配置
		val spark: SparkSession = {
			import com.jing.dmp.config.SparkConfigHelper._
			logWarning("通过隐式转换加载Spark Application配置参数信息完成..............")
			SparkSession.builder() // Builder
				.config(sparkConf)
				// 通过隐式转换调用工具类，加载配置参数信息
				.loadSparkConfig()
				.getOrCreate()
		}

		// 3、返回构建SparkSession实例对象
		spark
	}

}
