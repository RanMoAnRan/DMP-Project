package com.jing.dmp.process

import org.apache.spark.sql.DataFrame

/**
  * 所有Report生成，都继承自此类，这是一个公共的类，是一个基类
  */
trait ReportProcessor {

  // def reportGropupFields(): Seq[String]

  /**
    * 提供一个目标表名出去
    */
  def targetTableName(): String

  /**
    * 提供目标表的分区键
    */
  def targetTableKeys(): Seq[String]

  /**
    * 对外提供数据处理的过程：
    * 传递一个DataFrame, 经过处理以后, 将结果数据DataFrame保存
    */
  def processData(odsDF: DataFrame): Unit = {

    // a. 获取SparkSession实例对象
    val spark = odsDF.sparkSession
    import com.jing.dmp.utils.KuduUtils._

    // b、按照省市分组，统计结果(使用SQL语句分析)
    val reportDF: DataFrame = realProcessData(odsDF)

    // c. 结果数据保存到Kudu表中
    spark.createKuduTable(
      targetTableName(), reportDF.schema, targetTableKeys(), isDelete = false
    )
    reportDF
      .coalesce(1) // 由于将报表结果保存，数据量很少，最好降低分区数，提升性能
      .saveAsKuduTable(targetTableName())
  }

  /**
    * 每个报表子类必须实现的方法，真正的依据报表需求进行分析数据
    *
    * @param odsDF Kudu中ODS表
    */
  def realProcessData(odsDF: DataFrame): DataFrame
}
