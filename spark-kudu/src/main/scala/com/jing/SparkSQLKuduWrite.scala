package com.jing

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQLKuduWrite {
  def main(args: Array[String]): Unit = {

    // TODO: 1、构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._


    // TODO: 2、读取Kudu表的数据
    /*
      val TABLE_KEY = "kudu.table"
      val KUDU_MASTER = "kudu.master"
     */
    import org.apache.kudu.spark.kudu._
    val kuduDF: DataFrame = spark.read
      .option("kudu.table", "kudu_itcast_users")
      .option("kudu.master", "hadoop01:7051,hadoop02:7051,hadoop03:7051")
      .kudu

    kuduDF.printSchema()
    kuduDF.show(10, truncate = false)



    // 使用DSL数据转换
    val etlDF: DataFrame = kuduDF
      .select(
        $"id",
        when($"gender".equalTo("男"), "M").otherwise("F").as("gender")
      )



    // TODO: 将ETL后数据保存到Kudu表中
    etlDF
      .coalesce(1) // 降低分区数
      .write
      .mode(SaveMode.Append) // 对于Kudu来说，只能是Append模式，就相当于Kudu中Upsert
      .option("kudu.table", "kudu_itcast_users")
      .option("kudu.master", "hadoop01:7051,hadoop02:7051,hadoop03:7051")
      .kudu



    // 注册为临时视图
    kuduDF.createOrReplaceTempView("view_tmp_kudu")

    // 编写SQL分析
    spark.sql(
      """
        			  |SELECT
        			  |  id, (age + 1) AS age, name,
        			  |  CASE
        			  |    WHEN trim(gender) = "女" THEN "F" ELSE "M"
        			  |  END AS gender
        			  |FROM
        			  |  view_tmp_kudu
      			""".stripMargin)
      .show(10, truncate = false)

    // TODO：4、应用结束，关闭资源
    spark.stop()
  }
}
