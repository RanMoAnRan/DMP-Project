package com.jing.dmp.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 测试kuduhelper工具类
  */
object SparkKuduTest {

  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    import spark.implicits._

    val tableName: String = "kudu_students"
    val schema: StructType = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true)
      )
    )
    val keys: Seq[String] = Seq("id")

    import com.jing.dmp.utils.KuduUtils._
    // 实现：SparkSession.createKuduTable()
    //创建表
    //spark.createKuduTable(tableName, schema, keys, isDelete = false)

    //删除表
    // spark.deleteKuduTable(tableName)

    // 实现：DataFrame.saveAsKuduTable
    val studentsDF: DataFrame = Seq(
      (10001, "zhangsan", 23), (10002, "lisi", 22), (10003, "wagnwu", 23),
      (10004, "xiaohong", 21), (10005, "tainqi", 235), (10006, "zhaoliu", 24)
    ).toDF("id", "name", "age")

    //保存数据
    studentsDF.saveAsKuduTable(tableName)

    // 实现：SparkSession.readKuduTable(tableName)
    //读取数据
    spark.readKuduTable(tableName) match {
      case Some(df) => df.show(10, truncate = false)
      case None => println("没有数据。。。。。。。。。。")
    }

    // 应用结束，关闭资源
    spark.stop()
  }

}
