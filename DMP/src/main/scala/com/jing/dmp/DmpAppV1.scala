package com.jing.dmp

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * DMP项目主程序，包括数据ETL、报表统计分析及生成商圈库、数据标签化操作。（第一版）
  */
object DmpAppV1 {

	// 构建Logger实例对象，用于记录应用日志信息
	@transient private val logger: Logger = LoggerFactory.getLogger(
		this.getClass.getName.stripSuffix("$")
	)

	def main(args: Array[String]): Unit = {

		// 1、Spark Application相关参数设置
		val sparkConf: SparkConf = new SparkConf()
			.setAppName("DmpApplication")
			.setMaster("local[4]")
			.set("spark.worker.timeout", "600s")
			.set("spark.cores.max", "10")
			.set("spark.rpc.askTimeout", "600s")
			.set("spark.network.timeout", "600s")
			.set("spark.task.maxFailures", "5")
			.set("spark.speculation", "true")
			.set("spark.driver.allowMultipleContexts", "true")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.buffer.pageSize", "8m")
		// 2、构建SparkSession实例对象，传递应用配置
		val spark: SparkSession = SparkSession.builder()
	    	.config(sparkConf)
	    	.getOrCreate()

		val datasRDD: RDD[String] = spark.sparkContext.parallelize(List("aa", "bb", "cc"))

		datasRDD.foreach(data => {
			logger.info(s"info===========$data================")
			logger.warn(s"info===========$data================")
			logger.error(s"info===========$data================")
		})


		// 开发测试，为了查看WEB UI监控4040端口，让线程休眠
		Thread.sleep(100000)

		// 当应用运行完成，关闭资源
		spark.stop()
	}

}
