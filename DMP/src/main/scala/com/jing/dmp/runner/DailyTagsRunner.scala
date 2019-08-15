package com.jing.dmp.runner

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.tags.{HistoryTagsProcessor, MakeTagsProcessor}
import com.jing.dmp.utils.{DateUtils, SparkSessionUtils}
import org.apache.spark.sql.DataFrame

/**
  * 依据ODS表数据和商圈表数据，将数据进行标签化
  */
object DailyTagsRunner {

  // ODS表名
  val ODS_TABLE_NAME: String = AppConfigHelper.AD_MAIN_TABLE_NAME
  // Area的表名
  val AREA_TABLE_NAME: String = AppConfigHelper.BUSINESS_AREAS_TABLE_NAME
  //定义历史的标签数据表名
  val HISTORY_TAGS_TABLE_NAME: String = {
    AppConfigHelper.TAGS_TABLE_NAME_PREFIX + DateUtils.getYesterdayDate()
  }
  //定义当天的标签数据表明
  val TODAY_TAGS_TABLE = {
    AppConfigHelper.TAGS_TABLE_NAME_PREFIX + DateUtils.getTodayDate()
  }

  def main(args: Array[String]): Unit = {

    // 1、获取SparkSession实例对象
    val spark = SparkSessionUtils.createSparkSession(this.getClass)
    import com.jing.dmp.utils.KuduUtils._

    // 2、读取ODS表的广告数据和商圈表Area表的数据
    // 2.1 读取ODS表的数据
    val odsDFOption: Option[DataFrame] = spark.readKuduTable(ODS_TABLE_NAME)
    val odsDF: DataFrame = odsDFOption match {
      case Some(df) => df
      case None => println("ODS表的数据未读取到。。。。。。。。。。"); return
    }

    // 2.2 读取商圈表的数据
    val areaDFOption: Option[DataFrame] = spark.readKuduTable(AREA_TABLE_NAME)

    // 2.3 当商圈表数据存在的时候，进行关联（geoHash)
    val odsWithAreaDF: DataFrame = areaDFOption match {
      case Some(areaDF) =>
        odsDF.join(areaDF, odsDF.col("geoHash") === areaDF.col("geo_hash"), "left")
      case None => odsDF
    }

    // 3、调用标签处理类，转换生成标签数据DataFrame TODO：此处获取的是当日标签数据
    val tagsDF: DataFrame = MakeTagsProcessor.processData(odsWithAreaDF)
    tagsDF.show(10, truncate = false)


    // 4、获取历史标签数据，对历史标签数据中标签进行衰减（标签权重降低）
    val historyTagsDFOption: Option[DataFrame] = spark.readKuduTable(HISTORY_TAGS_TABLE_NAME)


    // 5、合并标签（今日标签和历史标签）
    val allTagsDF: DataFrame = historyTagsDFOption match {
      case Some(historyTagsDF) =>
        // 对历史标签中的数据（标签数据进行权重的衰减操作），与今日标签合并UNION
        tagsDF.union(HistoryTagsProcessor.processData(historyTagsDF))
      case None => tagsDF // 如果没有历标签数据，直接返回今日标签数据
    }

    // 6、保存标签数据到Kudu表中
    // a. 创建Kudu表中
    spark.createKuduTable(TODAY_TAGS_TABLE, allTagsDF.schema, Seq("main_id"))
    // b. dataframe保存到Kudu表
    allTagsDF.saveAsKuduTable(TODAY_TAGS_TABLE)

    // 开发测试，为了查看WEB UI监控4040端口，让线程休眠
    //Thread.sleep(1000000)

    // 当应用运行完成，关闭资源
    spark.stop()

  }

}
