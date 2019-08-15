package com.jing.dmp.runner

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.etl.IpProcessor
import com.jing.dmp.utils.SparkSessionUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

/**
  * DMP项目各个模块主程序，此处为数据ETL运行主程序类
	* 1. 创建 SparkSession
	* 2. 读取数据集
	* 3. 数据操作
	* 4. 数据落地
  */
object PmtETLRunner extends Logging{

	def main(args: Array[String]): Unit = {

		// 1、获取SparkSession实例对象
		val spark = SparkSessionUtils.createSparkSession(this.getClass)
		import spark.implicits._

		// 2、读取广告数据
		val pmtDF: DataFrame = spark.read.json(AppConfigHelper.AD_DATA_PATH)
		//pmtDF.printSchema()
		//pmtDF.show(10, truncate = false)

		// 3、对广告集成数据进行ETL操作（此处仅仅这对IP转换处理）
		val etlDF: DataFrame = IpProcessor.processData(pmtDF)
		etlDF.show(10, truncate = false) // 大概96个字段
		val schema: StructType = etlDF.schema

		// 4、选择指定字段值
		val selectColumns: Seq[Column] = Seq(
			'sessionid, 'advertisersid, 'adorderid, 'adcreativeid,
			'adplatformproviderid, 'sdkversion, 'adplatformkey,
			'putinmodeltype, 'requestmode, 'adprice, 'adppprice,
			'requestdate, 'ip, 'appid, 'appname, 'uuid, 'device,
			'client, 'osversion, 'density, 'pw, 'ph, 'longitude,
			'latitude, 'province, 'city, 'ispid, 'ispname, 'networkmannerid,
			'networkmannername, 'iseffective, 'isbilling, 'adspacetype,
			'adspacetypename, 'devicetype, 'processnode, 'apptype,
			'district, 'paymode, 'isbid, 'bidprice, 'winprice,
			'iswin, 'cur, 'rate, 'cnywinprice, 'imei, 'mac, 'idfa,
			'openudid, 'androidid, 'rtbprovince, 'rtbcity, 'rtbdistrict,
			'rtbstreet, 'storeurl, 'realip, 'isqualityapp, 'bidfloor, 'aw,
			'ah, 'imeimd5, 'macmd5, 'idfamd5, 'openudidmd5, 'androididmd5,
			'imeisha1, 'macsha1, 'idfasha1, 'openudidsha1, 'androididsha1,
			'uuidunknow, 'userid, 'reqdate, 'reqhour, 'iptype, 'initbidprice,
			'adpayment, 'agentrate, 'lomarkrate, 'adxrate, 'title, 'keywords,
			'tagid, 'callbackdate, 'channelid, 'mediatype, 'email, 'tel,
			'age, 'sex,'geoHash
		)
		val odsDF: DataFrame = etlDF.select(selectColumns: _*) // 将集合转换为一个个的元素传递到函数中

		// 4、保存DataFrame到Kudu表中
		import com.jing.dmp.utils.KuduUtils._
		val tableName = AppConfigHelper.AD_MAIN_TABLE_NAME
		// a. 创建Kudu表中
		spark.createKuduTable(tableName, odsDF.schema, Seq("uuid"))
		// b. dataframe保存到Kudu表
		odsDF.saveAsKuduTable(tableName)

		// 开发测试，为了查看WEB UI监控4040端口，让线程休眠
		//Thread.sleep(1000000)

		// 当应用运行完成，关闭资源
		spark.stop()
	}

}
