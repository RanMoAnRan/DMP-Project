package com.jing.dmp

import com.jing.dmp.config.AppConfigHelper
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * DMP项目主程序，包括数据ETL、报表统计分析及生成商圈库、数据标签化操作。(最终版)
  */
object DmpApp extends Logging {

  def main(args: Array[String]): Unit = {

    // 1、Spark Application相关参数设置
    val sparkConf: SparkConf = {
      val conf = new SparkConf()
        .setAppName(AppConfigHelper.SPARK_APP_NAME)
      // TODO: 判断是否是本地模式，如果是本地模式，设置值，否则不设置，通过提交应用--master设置
      if (AppConfigHelper.SPARK_APP_LOCAL_MODE.toBoolean) {
        //日志打印
        logInfo("Spark Application运行的本地模式。。")
        conf.setMaster(AppConfigHelper.SPARK_APP_MASTER)
      }
      conf
    }

    // 2、构建SparkSession实例对象，传递应用配置
    val spark: SparkSession = {
      //导入自定义隐式转换函数
      import com.jing.dmp.config.SparkConfigHelper._
      logWarning("通过隐式转换加载Spark Application配置参数信息完成。。")
      SparkSession.builder() // Builder
        .config(sparkConf)
        // 通过隐式转换调用工具类，加载配置参数信息
        .loadSparkConfig()
        .getOrCreate()
    }



    // 开发测试，为了查看WEB UI监控4040端口，让线程休眠
    Thread.sleep(1000000)

    // 当应用运行完成，关闭资源
    spark.stop()
  }

}
