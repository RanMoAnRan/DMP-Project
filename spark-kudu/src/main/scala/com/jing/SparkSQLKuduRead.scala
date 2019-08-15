package com.jing

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 使用SparkSession读取Kudu数据，封装到DataFrame/Dataset集合中
  * 更便捷的、与Spark风格一致的访问方式，是使用SparkSession访问Kudu；
  * 只需要在option中设置kudu.mater、kudu.table，非常便捷，推荐。
  */
object SparkSQLKuduRead {
  def main(args: Array[String]): Unit = {
    // TODO: 1、构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // TODO: 2、读取Kudu表中的数据
    import org.apache.kudu.spark.kudu._
    val kuduDF: DataFrame = spark.read
      .option("kudu.master", "hadoop01:7051,hadoop02:7051,hadoop03:7051")
      .option("kudu.table", "kudu_itcast_users")
      .kudu

    kuduDF.printSchema()
    kuduDF.show(10, truncate = false)

    // TODO：4、应用结束，关闭资源
    spark.stop()
  }

}
