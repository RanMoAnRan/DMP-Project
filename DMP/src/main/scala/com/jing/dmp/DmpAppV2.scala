package com.jing.dmp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * DMP项目主程序，包括数据ETL、报表统计分析及生成商圈库、数据标签化操作。（第二版）
  */
object DmpAppV2 {

	// 使用Config工具类库加载配置文件application.conf
	val config: Config = ConfigFactory.load()

	def main(args: Array[String]): Unit = {

		// 1、Spark Application相关参数设置
		val sparkConf: SparkConf = new SparkConf()
			.setAppName("DmpApplication")
			.setMaster("local[4]")
			.set("spark.worker.timeout", config.getString("spark.worker.timeout"))
			.set("spark.cores.max", config.getString("spark.cores.max"))
			.set("spark.rpc.askTimeout", config.getString("spark.rpc.askTimeout"))
			.set("spark.network.timeout", config.getString("spark.network.timeout"))
			.set("spark.task.maxFailures", config.getString("spark.task.maxFailures"))
			.set("spark.speculation", config.getString("spark.speculation"))
			.set("spark.driver.allowMultipleContexts", config.getString("spark.driver.allowMultipleContexts"))
			.set("spark.serializer", config.getString("spark.serializer"))
			.set("spark.buffer.pageSize", config.getString("spark.buffer.pageSize"))
		// 2、构建SparkSession实例对象，传递应用配置
		val spark: SparkSession = SparkSession.builder()
	    	.config(sparkConf)
	    	.getOrCreate()



		// 开发测试，为了查看WEB UI监控4040端口，让线程休眠
		Thread.sleep(1000000)

		// 当应用运行完成，关闭资源
		spark.stop()
	}

}
