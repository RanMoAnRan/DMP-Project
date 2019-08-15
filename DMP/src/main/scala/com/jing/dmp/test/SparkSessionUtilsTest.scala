package com.jing.dmp.test

import com.jing.dmp.utils.SparkSessionUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * DMP项目主程序，包括数据ETL、报表统计分析及生成商圈库、数据标签化操作。(最终版)
  */
object SparkSessionUtilsTest extends Logging {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSessionUtils.createSparkSession(this.getClass)


    // 开发测试，为了查看WEB UI监控4040端口，让线程休眠
    Thread.sleep(1000000)

    // 当应用运行完成，关闭资源
    spark.stop()
  }

}
