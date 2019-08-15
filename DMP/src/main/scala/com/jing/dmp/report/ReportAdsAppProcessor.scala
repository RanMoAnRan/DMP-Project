package com.jing.dmp.report

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.ReportProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
	* 广告投放的APP分布报表统计
	*/
object ReportAdsAppProcessor extends ReportProcessor{

	/**
	  * 业务分组字段
	  * @return
	  */
	def reportGropupFields(): Seq[String] = {
		Seq("appid", "appname")
	}

	/**
	  * 提供一个目标表名出去
	  */
	override def targetTableName(): String = {
		AppConfigHelper.REPORT_ADS_APP_TABLE_NAME
	}

	/**
	  * 提供目标表的分区键, 此时此函数可以放到父类Trait中实现
	  */
	override def targetTableKeys(): Seq[String] = {
		Seq("report_date") ++ reportGropupFields()
	}

	/**
	  * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
	  *
	  * @param odsDF Kudu中ODS表
	  */
	override def realProcessData(odsDF: DataFrame): DataFrame = {
		// 获取SparkSession实例对象
		val spark: SparkSession = odsDF.sparkSession

		// a. 将ODS表注册为临时视图
		odsDF.createOrReplaceTempView("view_temp_ods")

		// d. 编写SQL，计算竞价成功率、广告点击率和媒体点击率
		val reportDF = spark.sql(
			ReportSQLConstant.reportAdsKpiSQL("view_temp_ods", reportGropupFields())
		)

		// c. 返回统计报表
		reportDF
	}


	/**
	  * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
	  *
	  * @param odsDF Kudu中ODS表
	  */
	def realProcessDataV1(odsDF: DataFrame): DataFrame = {
		// 获取SparkSession实例对象
		val spark: SparkSession = odsDF.sparkSession

		// a. 将ODS表注册为临时视图
		odsDF.createOrReplaceTempView("view_temp_ods")

		// d. 编写SQL，计算竞价成功率、广告点击率和媒体点击率
		val reportDF = spark.sql(ReportSQLConstant.reportAdsAppSQLWith("view_temp_ods"))

		// c. 返回统计报表
		reportDF
	}
}
