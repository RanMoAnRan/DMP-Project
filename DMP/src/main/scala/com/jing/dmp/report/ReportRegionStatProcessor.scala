package com.jing.dmp.report

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.utils.SparkSessionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

/**
  * 各地域数量分布报表统计
  */
object ReportRegionStatProcessor {

  def main(args: Array[String]): Unit = {

    // 第一、创建SparkSession
    val spark = SparkSessionUtils.createSparkSession(this.getClass)
    // 导入隐式转换，函数类库，导入Kudu隐式转换函数
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import com.jing.dmp.utils.KuduUtils._

    // 第二、加载ods表的数据
    val odsDFOption: Option[DataFrame] = spark.readKuduTable(AppConfigHelper.AD_MAIN_TABLE_NAME)
    val odsDF: DataFrame = odsDFOption match {
      case Some(df) => df
      case None => println("ODS表的数据未读取到。。。。。。。。。。"); return
    }

    // 第三、按照省市分组，统计结果
    // 使用SQL语句分析
    // a. 注册为临时视图
    odsDF.createOrReplaceTempView("view_temp_ods")
    // b. 编写SQL执行（方式一）
    val reportDF: DataFrame = spark.sql(
      """
        			  |SELECT
        			  |  CAST(TO_DATE(NOW()) AS STRING)  AS report_date,
        			  |  province, city, COUNT(1) AS count
        			  |FROM
        			  |  view_temp_ods
        			  |GROUP BY
        			  |  province, city
      			""".stripMargin)
    //    reportDF.printSchema()
    //    reportDF.show(20, truncate = false)


    //DSL语句（方式二）
    val regionStateDF: DataFrame = odsDF
      // 指定分组字段进行聚合操作
      .groupBy($"province", $"city").count()
      // 使用函数获取报表数据产生日期及其他字段值
      .select(
      current_date().cast(StringType).as("report_date"),
      $"province", $"city", $"count"
    )

    regionStateDF.printSchema()
    regionStateDF.show(20, truncate = false)


    // 第四、结果数据保存到Kudu表中
    // a. 创建表, 当表不存在的时候进行创建
    val reportTableName = AppConfigHelper.REPORT_REGION_STAT_TABLE_NAME
    spark.createKuduTable(
      reportTableName, reportDF.schema,
      Seq("report_date", "province", "city"), isDelete = false
    )
    // b. 保存数据到Kudu表
    reportDF.saveAsKuduTable(reportTableName)


    // 当应用完成，关闭资源
    spark.stop()
  }

}
