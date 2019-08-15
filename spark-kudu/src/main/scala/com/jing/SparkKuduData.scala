package com.jing

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Kudu集成Spark，使用KuduContext对Kudu表中的数据进行操作
  */
object SparkKuduData {

  /** *
    * 将数据RDD插入到Kudu表中
    *
    * @param spark     SparkSession实例对象
    * @param context   KuduContext 实例上下文对象
    * @param tableName 表的名称
    */
  def insertData(spark: SparkSession, context: KuduContext, tableName: String) = {
    // a. 模拟产生数据
    val usersDF: DataFrame = spark.createDataFrame(
      Seq(
        (1001, "zhangsan", 23, "男"),
        (1002, "lisi", 22, "男"),
        (1003, "xiaohong", 24, "女"),
        (1005, "zhaoliu2", 33, "男")
      )
    ).toDF("id", "name", "age", "gender")

    // 插入数据方式一：如果行已经存在，INSERT将不允许插入行（导致失败）
    //context.insertRows(usersDF, tableName)

    // 插入数据方式一：将DataFrame的行插入Kudu表。如果行存在，则忽略插入动作。
    context.insertIgnoreRows(usersDF, tableName)
  }

  /** *
    * 从Kudu中查询数据, 表中有几个分区，查询的RDD就有几个分区
    *
    * @param spark     SparkSession实例对象
    * @param context   KuduContext 实例上下文对象
    * @param tableName 表的名称
    */
  def selectData(spark: SparkSession, context: KuduContext, tableName: String) = {
    // 指定获取的列名称
    val columnProjection: Seq[String] = Seq("id", "name", "age")
    /*
      def kuduRDD(sc: SparkContext,
              tableName: String,
              columnProjection: Seq[String] = Nil
          ): RDD[Row]
     */
    val datasRDD: RDD[Row] = context.kuduRDD(
      spark.sparkContext, tableName, columnProjection
    )

    // 数据打印出来
    datasRDD.foreachPartition { iter =>
      iter.foreach { row =>
        println(s"p-${TaskContext.getPartitionId()}: id = ${row.getInt(0)}" +
          s", name = ${row.getString(1)}, age = ${row.getInt(2)}")
      }
    }
  }

  /** *
    * 将数据RDD更新到到Kudu表中
    *
    * @param spark     SparkSession实例对象
    * @param context   KuduContext 实例上下文对象
    * @param tableName 表的名称
    */
  def updateData(spark: SparkSession, context: KuduContext, tableName: String) = {
    // a. 模拟产生数据
    val usersDF: DataFrame = spark.createDataFrame(
      Seq(
        (1001, "zhangsan22", 24, "男"),
        (1002, "lisi22", 23, "男"),
        (1003, "xiaohong11", 24, "女"),
        (1005, "zhaoliu244", 33, "男")
      )
    ).toDF("id", "name", "age", "gender")

    // 数据存在做update；不存在报错
    context.updateRows(usersDF, tableName)
  }

  /** *
    * 将数据RDD更新插入到到Kudu表中，存在为更新，不存在为插入
    *
    * @param spark     SparkSession实例对象
    * @param context   KuduContext 实例上下文对象
    * @param tableName 表的名称
    */
  def upsertData(spark: SparkSession, context: KuduContext, tableName: String) = {
    // a. 模拟产生数据
    val usersDF: DataFrame = spark.createDataFrame(
      Seq(
        (1001, "zhangsa风", 24, "男"),
        (1006, "tianqi", 33, "男")
      )
    ).toDF("id", "name", "age", "gender")

    // 数据存在做update；不存在不报错
    context.upsertRows(usersDF, tableName)
  }

  /** *
    * 删除Kudu表中的数据
    *
    * @param spark     SparkSession实例对象
    * @param context   KuduContext 实例上下文对象
    * @param tableName 表的名称
    */
  def deleteData(spark: SparkSession, context: KuduContext, tableName: String) = {
    import spark.implicits._
    //根据id进行删除
    val usersDF: DataFrame = spark.sparkContext
      .parallelize(List(1006))
      .toDF("id")

    // 数据删除操作
    context.deleteRows(usersDF, tableName)
  }

  def main(args: Array[String]): Unit = {
    // TODO: 1、构建SparkSession实例对象
    val sparkConf = new SparkConf()
      .setMaster("local[4]").setAppName(this.getClass.getSimpleName)
    // 采用建造者模式创建SparkSession实例
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf) // 设置应用配置信息
      .getOrCreate()
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // TODO: 2、构建KuduContext实例对象，用以操作Kudu中表的数据
    val kuduMaster = "hadoop01:7051,hadoop02:7051,hadoop03:7051"
    val kuduContext: KuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    val tableName = "kudu_itcast_users"

    // 插入数据
    //insertData(spark, kuduContext, tableName)

    // 查询数据
    //selectData(spark, kuduContext, tableName)

    // 更新数据
    updateData(spark, kuduContext, tableName)

    // 插入更新数据
    //upsertData(spark, kuduContext, tableName)

    // 删除数据
    //deleteData(spark, kuduContext, tableName)

    // TODO：4、应用结束，关闭资源
    spark.stop()
  }
}
