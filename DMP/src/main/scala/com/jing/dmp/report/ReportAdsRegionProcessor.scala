package com.jing.dmp.report

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.ReportProcessor
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReportAdsRegionProcessor extends ReportProcessor {

  /**
    * 提供一个目标表名出去
    */
  override def targetTableName(): String = {
    AppConfigHelper.REPORT_ADS_REGION_TABLE_NAME
  }

  /**
    * 提供目标表的分区键
    */
  override def targetTableKeys(): Seq[String] = {
    Seq("report_date", "province", "city")
  }

  /**
    * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
    *
    * @param odsDF Kudu中ODS表
    */
  override def realProcessData(odsDF: DataFrame): DataFrame = {
    // 获取SparkSession实例对象
    val spark: SparkSession = odsDF.sparkSession

    // 可以调用SQL分析，也可以调用DSL分析
    //val reportDF = realProcessDataV1(odsDF)
    val reportDF = realProcessDataDSL(odsDF)

    // c. 返回统计报表
    reportDF
  }

  /**
    * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
    *
    * @param odsDF Kudu中ODS表
    */
  def realProcessDataSQL(odsDF: DataFrame): DataFrame = {
    // 获取SparkSession实例对象
    val spark: SparkSession = odsDF.sparkSession

    // a. 将ODS表注册为临时视图
    odsDF.createOrReplaceTempView("view_temp_ods")

    // d. 编写SQL，计算竞价成功率、广告点击率和媒体点击率
    //val reportDF = spark.sql(ReportSQLConstant.reportAdsRegionSQLSub("view_temp_ods"))
    val reportDF = spark.sql(ReportSQLConstant.reportAdsRegionSQLWith("view_temp_ods"))

    // c. 返回统计报表
    reportDF
  }

  /**
    * 采用DSL编程，对广告区域报表统计
    */
  def realProcessDataDSL(odsDF: DataFrame): DataFrame = {
    // 1. 导入隐式转换和函数库
    import odsDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // 2. 采用DSL（调用Dataset中函数）
    odsDF
      // a. 按照字段分组： province和city
      .groupBy($"province", $"city")
      // b. 使用聚合函数统计 agg, 调用函数库
      .agg(
      sum(
        when(
          $"requestmode".equalTo(1).and($"processnode".geq(1)), 1
        ).otherwise(0)
      ).as("orginal_req_cnt"),
      sum(
        when(
          $"requestmode".equalTo(1).and($"processnode".geq(2)), 1
        ).otherwise(0)
      ).as("valid_req_cnt"),
      sum(
        when(
          $"requestmode".equalTo(1).and($"processnode".equalTo(3)), 1
        ).otherwise(0)
      ).as("ad_req_cnt"),
      sum(
        when(
          $"adplatformproviderid".geq(100000).and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1)).and($"isbid".equalTo(1))
            .and($"adorderid".notEqual(0)), 1
        ).otherwise(0)
      ).as("join_rtx_cnt"),
      sum(
        when(
          $"adplatformproviderid".geq(100000).and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1)).and($"iswin".equalTo(1)), 1
        ).otherwise(0)
      ).as("success_rtx_cnt"),
      sum(
        when(
          $"requestmode".equalTo(2).and($"iseffective".equalTo(1)), 1
        ).otherwise(0)
      ).as("ad_show_cnt"),
      sum(
        when(
          $"requestmode".equalTo(3).and($"iseffective".equalTo(1)), 1
        ).otherwise(0)
      ).as("ad_click_cnt"),
      sum(
        when(
          $"requestmode".equalTo(2).and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1)), 1
        ).otherwise(0)
      ).as("media_show_cnt"),
      sum(
        when(
          $"requestmode".equalTo(3).and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1)), 1
        ).otherwise(0)
      ).as("media_click_cnt"),
      sum(
        when(
          $"adplatformproviderid".geq(100000).and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1)).and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000)).and($"adcreativeid".gt(200000)), $"winprice" / 1000
        ).otherwise(0)
      ).as("dsp_pay_money"),
      sum(
        when(
          $"adplatformproviderid".geq(100000).and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1)).and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000)).and($"adcreativeid".gt(200000)), $"adpayment" / 1000
        ).otherwise(0)
      ).as("dsp_cost_money")
    )
      // c. 过滤为0的值
      .filter(
      $"join_rtx_cnt" =!= 0 && $"success_rtx_cnt" =!= 0 &&
        $"ad_show_cnt" =!= 0 && $"ad_click_cnt" =!= 0 &&
        $"media_show_cnt" =!= 0 && $"media_click_cnt" =!= 0
    )
      // d. 选取字段值，并且计算广告点击率，媒体点击率和竞价成功率
      .select(
      current_date().cast(StringType).as("report_date"), $"*",
      round($"success_rtx_cnt" / $"join_rtx_cnt", 2).as("success_rtx_rate"),
      round($"ad_click_cnt" / $"ad_show_cnt", 2).as("ad_click_rate"),
      round($"media_click_cnt" / $"media_show_cnt", 2).as("media_click_rate")
    )
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

    // b. 编写SQL并执行
    val regionReportDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsRegionSQL("view_temp_ods"))

    // c. 注册为临时视图
    regionReportDF.createOrReplaceTempView("view_temp_ads_region")

    // d. 编写SQL，计算竞价成功率、广告点击率和媒体点击率
    val reportDF = spark.sql(ReportSQLConstant.reportAdsRegionRateSQL("view_temp_ads_region"))
    reportDF.printSchema()
    reportDF.show(10, truncate = false)

    // e. 返回分析结果DataFrame
    reportDF
  }
}
