package com.jing.dmp.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

class KuduUtils extends Logging {

  // 加载Kudu相关配置文件，获取Kudu Master地址和Tablet副本数目
  val kuduConfig = ConfigFactory.load("kudu.conf")

  var spark: SparkSession = _
  var kuduContext: KuduContext = _

  var dataframe: DataFrame = _

  // TODO: 定义附属构造方法
  def this(sparkSession: SparkSession) {
    this() // 调用主构造方法
    this.spark = sparkSession
    this.kuduContext = new KuduContext(
      kuduConfig.getString("kudu.master"), this.spark.sparkContext
    )
  }

  def this(df: DataFrame) {
    this(df.sparkSession) // 调用附属构造方法
    this.dataframe = df
  }

  /**
    * 创建Kudu中的表，当表存在时且允许删除时，先删除再创建
    *
    * @param tableName 表的名称
    * @param schema    表的Schema信息
    * @param keys      表的主键
    * @param isDelete  表若存在是否允许删除
    */
  def createKuduTable(tableName: String, schema: StructType,
                      keys: Seq[String], isDelete: Boolean = true): Unit = {
    // a. 判断要创建的表是否存在，如果存在的话，判断是否可以删除，如果可以删除，先删除再创建
    if (kuduContext.tableExists(tableName)) {
      if (isDelete) {
        kuduContext.deleteTable(tableName)
        logInfo(s"Kudu中表${tableName}存在，已被删除........")
      } else {
        logInfo(s"Kudu中表${tableName}存在，不允许删除........")
        return
      }
    }

    // b. 构建Kudu表中基本设置，如分区策略和副本数
    val options: CreateTableOptions = new CreateTableOptions()
    // 设置副本数目
    options.setNumReplicas(kuduConfig.getInt("kudu.table.factor"))
    // 设置分区策略，此处一律采用hash分区策略，使用keys主键进行
    import scala.collection.JavaConverters._
    options.addHashPartitions(keys.asJava, 3)

    /*
      def createTable(
          tableName: String,
        schema: StructType,
        keys: Seq[String],
        options: CreateTableOptions
      ): KuduTable
     */
    kuduContext.createTable(tableName, schema, keys, options)
  }

  /**
    * 删除Kudu中存在的表
    *
    * @param tableName 表的名称
    */
  def deleteKuduTable(tableName: String): Unit = {
    // 判断表是否存在，存在即删除
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
      logInfo(s"Kudu中表：${tableName}存在，已删除成功..............")
    }
  }


  /**
    * SparkSession从Kudu表中读取数据
    *
    * @param tableName 表的名称
    * @return
    */
  def readKuduTable(tableName: String): Option[DataFrame] = {
    // a. 判断读取Kudu表是否存在
    if (kuduContext.tableExists(tableName)) {
      // a.1 导入隐式函数
      import org.apache.kudu.spark.kudu.KuduDataFrameReader
      // a.2 读取Kudu表数据
      val kuduDF: DataFrame = spark.read
        .option("kudu.master", kuduConfig.getString("kudu.master"))
        .option("kudu.table", tableName)
        .kudu
      // b. 返回
      Some(kuduDF)
    } else {
      None
    }
  }


  /**
    * 将DataFrame数据保存到Kudu表中
    *
    * @param tableName 表的名称
    */
  def saveAsKuduTable(tableName: String): Unit = {
    // a. 导入隐式函数
    import org.apache.kudu.spark.kudu.KuduDataFrameWriter
    // b. 保存数据
    dataframe
      .coalesce(1) // 降低分区数
      .write
      .mode(SaveMode.Append)
      .option("kudu.master", kuduConfig.getString("kudu.master"))
      .option("kudu.table", tableName)
      .kudu
  }


}

object KuduUtils {

  // TODO: 隐式转换 -> 将SparkSession转换为KuduHelper实例对象
  implicit def sparkSessionToKuduHelper(spark: SparkSession): KuduUtils = {
    new KuduUtils(spark)
  }

  // TODO: 隐式转换 -> 将DataFrame转换为KuduHelper实例对象
  implicit def dataframeToKuduHelper(dataframe: DataFrame): KuduUtils = {
    if (dataframe == null) {
      throw new RuntimeException("数据集DataFrame为null，请创建DataFrame再保存数据")
    }
    new KuduUtils(dataframe)
  }

}
