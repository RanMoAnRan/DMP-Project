package com.jing.dmp.report

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.ReportProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 网络运营商维度：广告投放的网络运营商分布
		- 网络运营商ID和名称：ispid, ispname
  */
object ReportAdsIspProcessor extends ReportProcessor {
	/**
	  * 提供一个目标表名出去
	  */
	override def targetTableName(): String = {
		AppConfigHelper.REPORT_ADS_ISP_TABLE_NAME
	}

	/**
	  * 提供目标表的分区键
	  */
	override def targetTableKeys() = {
		Seq("report_date", "ispid", "ispname")
	}

	/**
	  * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
	  *
	  * @param odsDF Kudu中ODS表
	  */
	override def realProcessData(odsDF: DataFrame) = {
		// 获取SparkSession实例对象
		val spark: SparkSession = odsDF.sparkSession

		// a. 将ODS表注册为临时视图
		odsDF.createOrReplaceTempView("view_temp_ods")

		// d. 编写SQL，计算竞价成功率、广告点击率和媒体点击率
		val reportDF = spark.sql(
			ReportSQLConstant.reportAdsKpiSQL("view_temp_ods", Seq("ispid", "ispname"))
		)

		// c. 返回统计报表
		reportDF
	}
}
