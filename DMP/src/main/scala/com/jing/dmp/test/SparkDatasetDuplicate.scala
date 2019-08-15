package com.jing.dmp.test

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.utils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * 主要测试Dataset中一类API使用，去重函数
  */
object SparkDatasetDuplicate {

  def main(args: Array[String]): Unit = {

    // 1、获取SparkSession实例对象
    val spark = SparkSessionUtils.createSparkSession(this.getClass)
    import com.jing.dmp.utils.KuduUtils._
    import spark.implicits._

    // 2、读取ODS表的广告数据
    val odsDFOption: Option[DataFrame] = spark.readKuduTable(
      AppConfigHelper.AD_MAIN_TABLE_NAME
    )
    val odsDF: DataFrame = odsDFOption match {
      case Some(df) => df
      case None => println("ODS表的数据未读取到。。。。。。。。。。"); return
    }


    val geoHashDF: DataFrame = odsDF
      .select($"ip", $"geoHash", $"longitude", $"latitude")
      .orderBy($"ip".desc)
    geoHashDF.show(30, truncate = false)

    println(s"ODS表的原始条目数：${geoHashDF.count()}")

    // TODO: 调用 distinct 函数去重
    val distDF: Dataset[Row] = geoHashDF.distinct()
    distDF.show(30, truncate = false)
    println(s"ODS表的去重以后条目数：${distDF.count()}")


    println("=================================================")

    // TODO: 调用dropDuplicates函数指定列名，进行去重
    val distDF2: Dataset[Row] = geoHashDF.dropDuplicates(Seq("geoHash"))
    distDF2.show(30, truncate = false)
    println(s"ODS表按照geoHash进行去重以后条目数：${distDF2.count()}")

    println("=================================================")


    // TODO: 调用drop函数，删除指定列名的列
    val dropDF: DataFrame = geoHashDF.drop($"ip")
    dropDF.printSchema()
    dropDF.show(10, truncate = false)


    // 开发测试，为了查看WEB UI监控4040端口，让线程休眠
    //Thread.sleep(1000000)

    // 当应用运行完成，关闭资源
    spark.stop()
  }

}
