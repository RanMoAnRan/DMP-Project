package com.jing.dmp


import com.jing.dmp.config.AppConfigHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * DMP项目主程序，包括数据ETL、报表统计分析及生成商圈库、数据标签化操作。(第三版)
  */
object DmpAppV3 {

  def main(args: Array[String]): Unit = {

    // 1、Spark Application相关参数设置
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(AppConfigHelper.SPARK_APP_NAME)
      .setMaster(AppConfigHelper.SPARK_APP_MASTER)
      .setAll(AppConfigHelper.SPARK_APP_PARAMS)
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
