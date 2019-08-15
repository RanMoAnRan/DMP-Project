package com.jing.dmp.runner

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.report.{ReportAdsAppProcessor, ReportAdsChannelProcessor, ReportAdsDeviceProcessor, ReportAdsIspProcessor, ReportAdsNetworkProcessor, ReportAdsRegionProcessor, ReportRegionProcessor}
import com.jing.dmp.utils.SparkSessionUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * DMP 项目中专门报表生成主类Runner
  */
object DailyReportRunner extends Logging {

  def main(args: Array[String]): Unit = {

    // 1、获取SparkSession实例对象
    val spark = SparkSessionUtils.createSparkSession(this.getClass)
    import com.jing.dmp.utils.KuduUtils._

    // 2、读取ODS表的广告数据
    val odsDFOption: Option[DataFrame] = spark.readKuduTable(AppConfigHelper.AD_MAIN_TABLE_NAME)
    val odsDF: DataFrame = odsDFOption match {
      case Some(df) => df
      case None => println("ODS表的数据未读取到.............."); return
    }

    // 由于所有报表对ODS数据统计分析，所以缓存
    odsDF.persist(StorageLevel.MEMORY_AND_DISK)

    // TODO: 不同业务类型报表分析，并将结果数据存储Kudu表中
    // a. 各地域分布统计：region_stat_analysis
    ReportRegionProcessor.processData(odsDF)
    // b. 广告区域统计：ads_region_analysis
    ReportAdsRegionProcessor.processData(odsDF)
    // c. 广告APP统计：ads_app_analysis
    ReportAdsAppProcessor.processData(odsDF)
    // d. 广告设备统计：ads_device_analysis
    ReportAdsDeviceProcessor.processData(odsDF)
    // e. 广告网络类型统计：ads_network_analysis
    ReportAdsNetworkProcessor.processData(odsDF)
    // f. 广告运营商统计：ads_isp_analysis
    ReportAdsIspProcessor.processData(odsDF)
    // g. 广告渠道统计：ads_channel_analysis
    ReportAdsChannelProcessor.processData(odsDF)

    // 释放缓存数据
    odsDF.unpersist()

    // 开发测试，为了查看WEB UI监控4040端口，让线程休眠
    //Thread.sleep(1000000)

    // 当应用运行完成，关闭资源
    spark.stop()
  }

}
