package com.jing.dmp.config

import com.jing.dmp.utils.DateUtils
import com.typesafe.config.ConfigFactory

object AppConfigHelper {

	private val config = ConfigFactory.load()

	//  App Info
	lazy val SPARK_APP_NAME: String = config.getString("spark.app.name")
	// Spark Local Mode
	lazy val SPARK_APP_LOCAL_MODE: String = config.getString("spark.local.mode")
	// Spark parameters
	lazy val SPARK_APP_MASTER: String = config.getString("spark.master")

	lazy val SPARK_APP_PARAMS: List[(String, String)] = List(
		("spark.worker.timeout", config.getString("spark.worker.timeout")),
		("spark.cores.max", config.getString("spark.cores.max")),
		("spark.rpc.askTimeout", config.getString("spark.rpc.askTimeout")),
		("spark.network.timeout", config.getString("spark.network.timeout")),
		("spark.task.maxFailures", config.getString("spark.task.maxFailures")),
		("spark.speculation", config.getString("spark.speculation")),
		("spark.driver.allowMultipleContexts", config.getString("spark.driver.allowMultipleContexts")),
		("spark.serializer", config.getString("spark.serializer")),
		("spark.buffer.pageSize", config.getString("spark.buffer.pageSize"))
	)

	// kudu parameters
	lazy val KUDU_MASTER: String = config.getString("kudu.master")

	// input dataset
	lazy val AD_DATA_PATH: String = config.getString("addata.path")
	lazy val IPS_DATA_GEO_PATH: String = config.getString("ipdata.geo.path")
	lazy val IPS_DATA_REGION_PATH: String = config.getString("ipdata.region.path")

	// output dataset
	private lazy val DELIMITER = "_"
	private lazy val ODS_PREFIX: String = config.getString("ods.prefix")
	private lazy val AD_INFO_TABLE_NAME: String = config.getString("ad.data.tablename")
	lazy val AD_MAIN_TABLE_NAME = s"$ODS_PREFIX$DELIMITER$AD_INFO_TABLE_NAME$DELIMITER${DateUtils.getTodayDate()}"

	// report
	lazy val REPORT_REGION_STAT_TABLE_NAME: String = config.getString("report.region.stat.tablename")
	lazy val REPORT_ADS_REGION_TABLE_NAME: String = config.getString("report.ads.region.tablename")
	lazy val REPORT_ADS_APP_TABLE_NAME: String = config.getString("report.ads.app.tablename")
	lazy val REPORT_ADS_DEVICE_TABLE_NAME: String = config.getString("report.ads.device.tablename")
	lazy val REPORT_ADS_NETWORK_TABLE_NAME: String = config.getString("report.ads.network.tablename")
	lazy val REPORT_ADS_ISP_TABLE_NAME: String = config.getString("report.ads.isp.tablename")
	lazy val REPORT_ADS_CHANNEL_TABLE_NAME: String = config.getString("report.ads.channel.tablename")

	// GaoDe AMAP API
	private lazy val AMAP_KEY: String = config.getString("amap.key")
	private lazy val AMAP_BASE_URL: String = config.getString("amap.base.url")
	lazy val AMAP_URL: String = s"$AMAP_BASE_URL&key=$AMAP_KEY&location="

	// GeoHash
	lazy val KEY_LENGTH: Int = config.getInt("geohash.key.length")

	// 商圈库
	lazy val BUSINESS_AREAS_TABLE_NAME: String =config.getString("business.areas.tablename")

	// tags
	lazy val ID_FIELDS: String = config.getString("non.empty.field")
	lazy val FILTER_SQL: String = ID_FIELDS
		.split(",")
		.map(field => s"$field is not null ")
		.mkString(" or ")
	lazy val APP_NAME_DIC: String = config.getString("appname.dic.path")
	lazy val DEVICE_DIC: String = config.getString("device.dic.path")
	lazy val TAGS_TABLE_NAME_PREFIX: String = config.getString("tags.table.name.prefix") + DELIMITER
	lazy val TAG_COEFF: Double = config.getDouble("tag.coeff")

	// 加载 elasticsearch 相关参数
	lazy val ES_SPARK_PARAMS = List(
		("cluster.name", config.getString("es.cluster.name")),
		("es.index.auto.create", config.getString("es.index.auto.create")),
		("es.nodes", config.getString("es.Nodes")),
		("es.port", config.getString("es.port")),
		("es.index.reads.missing.as.empty", config.getString("es.index.reads.missing.as.empty")),
		("es.nodes.discovery", config.getString("es.nodes.discovery")),
		("es.nodes.wan.only", config.getString("es.nodes.wan.only")),
		("es.http.timeout", config.getString("es.http.timeout"))
	)

}
