package com.jing.dmp.report

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.ReportProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

/**
  * 报表统计：各地域数量分布报表统计(两种方式SQL和DSL)
  */
object ReportRegionProcessor extends ReportProcessor {


  override def targetTableName() = {
    AppConfigHelper.REPORT_REGION_STAT_TABLE_NAME
  }

  override def targetTableKeys() = {
    Seq("report_date", "province", "city")
  }


  /**
    * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
    *
    * @param odsDF Kudu中ODS表
    */
  override def realProcessData(odsDF: DataFrame) = {
    // 导入隐式转换
    import odsDF.sparkSession.implicits._

    // a. 导入SparkSQL函数库
    import org.apache.spark.sql.functions._

    // b. 编写DSL编程
    val reportDF = odsDF
      // 分组统计
      .groupBy($"province", $"city").count()
      // 选取字段，添加report_date（报表生成时间字段）
      .select(
      current_date().cast(StringType).as("report_date"),
      $"province", $"city", $"count"
    )

    // c. 返回报表DataFrame
    reportDF
  }


  def realProcessDataV1(odsDF: DataFrame) = {
    // 注册临时视图
    odsDF.createOrReplaceTempView("view_temp_ods")
    // 编写SQL执行
    val reportDF: DataFrame = odsDF.sparkSession.sql(
      """
        			  |SELECT
        			  |  CAST(TO_DATE(NOW()) AS STRING)  AS report_date,
        			  |  province, city, COUNT(1) AS count
        			  |FROM
        			  |  view_temp_ods
        			  |GROUP BY
        			  |  province, city
      			""".stripMargin)
    // 返回报表DataFrame
    reportDF
  }
}
