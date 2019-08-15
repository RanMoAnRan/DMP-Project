package com.jing



import org.apache.kudu.client.{CreateTableOptions, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 使用KuduContext创建Kudu中的表
  */
object SparkKuduTable {

  /**
    * 创建Kudu中的表
    *
    * @param context   KuduContext实例对象
    * @param tableName 表的名称
    */
  def createKuduTable(context: KuduContext, tableName: String): Unit = {
    // a. 定义Schema信息
    val schema: StructType = StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("gender", StringType, nullable = true)
      )
    )
    // b. 设置主键Key
    val keys: Seq[String] = Seq("id")
    // c. 设置分区策略及副本数
    val options: CreateTableOptions = new CreateTableOptions()
    // 使用哈希分区策略
    /*val columns: util.List[String] = new util.ArrayList[String]()
    columns.add("id")
    options.addHashPartitions(columns, 3)*/

    // 导入Scala与Java集合隐式转换
    import scala.collection.JavaConverters._
    options.addHashPartitions(List("id").asJava, 3)

    // 设置副本数为3，必须为奇数
    options.setNumReplicas(1)
    /*
      def createTable(tableName: String,
                  schema: StructType,
                  keys: Seq[String],
                  options: CreateTableOptions): KuduTable
     */
    val kuduTable: KuduTable = context.createTable(tableName, schema, keys, options)
    println(kuduTable)
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

    // TODO：3、创建表
    createKuduTable(kuduContext, "kudu_itcast_users")

    // TODO：4、应用结束，关闭资源
    spark.stop()
  }

}
