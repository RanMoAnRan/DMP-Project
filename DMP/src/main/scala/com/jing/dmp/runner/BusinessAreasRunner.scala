package com.jing.dmp.runner

import com.jing.dmp.area.AreaProcessor
import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.utils.SparkSessionUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
  * 根据经纬度, 查询高德的 API 获取商圈信息, 生成数据库放置商圈信息
  */
object BusinessAreasRunner extends Logging {

	// ODS 表，每日处理前一条ODS数据
	private val SOURCE_TABLE_NAME = AppConfigHelper.AD_MAIN_TABLE_NAME
	// 商圈库表名称
	private val SINK_TABLE_NAME = AppConfigHelper.BUSINESS_AREAS_TABLE_NAME

	def main(args: Array[String]): Unit = {

		// 1、获取SparkSession实例对象
		val spark = SparkSessionUtils.createSparkSession(this.getClass)
		import com.jing.dmp.utils.KuduUtils._

		// 2、读取ODS表的广告数据
		val odsDFOption: Option[DataFrame] = spark.readKuduTable(SOURCE_TABLE_NAME)
		val odsDF: DataFrame = odsDFOption match {
			case Some(df) => df
			case None => println("ODS表的数据未读取到。。。。。。。。。。"); return
		}


		// 3、获取商圈库信息
		val areaDF: DataFrame = AreaProcessor.processData(odsDF)
		areaDF.show(20, truncate = false)

		// 4、保存DataFrame到Kudu表中
		// a. 创建Kudu表中
		spark.createKuduTable(SINK_TABLE_NAME, areaDF.schema, Seq("geo_hash"), isDelete = false)
		// b. dataframe保存到Kudu表
		areaDF.saveAsKuduTable(SINK_TABLE_NAME)

		// 开发测试，为了查看WEB UI监控4040端口，让线程休眠
		//Thread.sleep(1000000)

		// 当应用运行完成，关闭资源
		spark.stop()

	}

}
