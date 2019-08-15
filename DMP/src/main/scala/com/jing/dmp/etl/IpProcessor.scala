package com.jing.dmp.etl

import com.jing.dmp.beans.IpRegion
import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.Processor
import com.jing.dmp.utils.IPUtils
import com.maxmind.geoip.LookupService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  * 对原数据中的IP地址进行转换处理
  * 生成新的dataframe返回
  */
object IpProcessor extends Processor {

  override def processData(pmtDF: DataFrame): DataFrame = {
    val spark = pmtDF.sparkSession
    // 导入隐式转换
    import spark.implicits._

    // 1. 将DataFrame转换为RDD，针对RDD每个分区数据进行操作,其中数据类型为Row
    /**
      * 由于DataFrame是弱类型（Row），会
      * 导致异常，建议要么将DataFrame转换为Dataset，要么转换为RDD操作。
      * 此处转换为rdd操作
      */
    val newRowsRDD: RDD[Row] = pmtDF.rdd.mapPartitions { datas =>

      // 构建IP地址解析工具类实例对象
      val dbSearcher: DbSearcher = new DbSearcher(
        new DbConfig(), AppConfigHelper.IPS_DATA_REGION_PATH
      )
      // 获取IP转换经纬度实例对象
      val lookupService: LookupService = new LookupService(
        AppConfigHelper.IPS_DATA_GEO_PATH, LookupService.GEOIP_MEMORY_CACHE
      )

      // 对每个分区的数据IP地址转换操作
      datas.map { row =>
        // a.获取每条数据中IP地址值
        val ipValue = row.getAs[String]("ip")

        // b. 调用工具类，解析IP地址值为省份、城市、经纬度和GeoHash值
        val region: IpRegion = IPUtils.convertIp2Region(ipValue, dbSearcher, lookupService)

        // c. 将解析的值追加到原始Row中, 此处使用 Seq中函数 :+
        val newRow: Seq[Any] = row.toSeq :+
          region.longitude :+
          region.latitude :+
          region.province :+
          region.city :+
          region.geoHash

        //  d. 直接返回Row对象
        Row.fromSeq(newRow)
      }
    }

    // 2. 自定义Schema类型
    val newSchema: StructType = pmtDF.schema
      .add("longitude", DoubleType, nullable = true)
      .add("latitude", DoubleType, nullable = true)
      .add("province", StringType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("geoHash", StringType, nullable = true)

    // 3. 构建新的DataFrame, 直接返回
    spark.createDataFrame(newRowsRDD, newSchema)
  }
}
