package com.jing.dmp.area

import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.Processor
import com.jing.dmp.utils.HttpUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 依据经纬度拼接请求URL，调用高德API，解析JSON数据，获取商圈信息
  */
object AreaProcessor extends Processor {
  // 构建Logger实例对象，用于记录应用日志信息
  @transient private val logger: Logger = LoggerFactory.getLogger(
    this.getClass.getName.stripSuffix("$")
  )

  /**
    * 开发步骤如下：
    * 1、获取GeoHash值、经度和维度
    * 2、依据经纬度范围，过滤非大陆的数据
    * 3、按照GeoHash去重（使用group by），获取组内经纬度平均值
    * 4、获取已有商圈信息
    * 5、与ODS表关联（左外连接），获取新的经纬度信息
    * 6、自定义UDF，传递经纬度，调用高德API，获取商圈信息
    * 7、保存商圈信息到Kudu表（GeoHash，Area）
    */
  override def processData(odsDF: DataFrame): DataFrame = {

    val spark: SparkSession = odsDF.sparkSession
    // 第一步、到如隐式转换和函数库
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 第二步、注册UDF函数，传递经纬度获取商圈信息
    val get_area: UserDefinedFunction = udf(HttpUtils.loadGaode2Area _) // 当传递函数名称到高阶函数中时，需求使用空格下划线

    // 第三步、将ODS表的数据进行过滤、分组处理DataFrame
    val geoHashDF: DataFrame = odsDF
      // 1、获取GeoHash值、经度和维度
      .select($"geoHash", $"longitude", $"latitude")
      // 2、依据经纬度范围，过滤非大陆的数据, 经度73.66~135.05，纬度3.86~53.55
      .filter(
      $"longitude".geq(73.66).and($"longitude".leq(135.05))
        .and($"latitude".geq(3.86)).and($"latitude".leq(53.55))
    )
      // 3、按照GeoHash去重（使用group by），获取组内经纬度平均值
      .groupBy($"geoHash")
      .agg(
        round(avg($"longitude"), 6).as("longitude"), // 同一个GeoHash对应的所有的经度平均值
        round(avg($"latitude"), 6).as("latitude") // 同一个GeoHash对应的所有的维度平均值
      )

    logger.warn(s"从Kudu读取ODS表数据量：${odsDF.count()}")
    logger.warn(s"对ODS表数据经过过滤去重后数据量：${geoHashDF.count()}")

    // 第四步、获取已有的商圈库数据（第一次没有）
    import com.jing.dmp.utils.KuduUtils._
    val areaDFOption: Option[DataFrame] = spark.readKuduTable(
      AppConfigHelper.BUSINESS_AREAS_TABLE_NAME
    )

    var businessAreaDF: DataFrame = null
    // 第五步、判断商圈库数据是否存在，如果存在，对处理后的ODS数据进行关联去除已存在商圈信息经纬度
    if (areaDFOption.isDefined) {
      // 4、获取已有商圈信息
      val areaDF: DataFrame = areaDFOption.get

      businessAreaDF = geoHashDF
        // 5、与ODS表关联（左外连接），获取新的经纬度信息
        .join(
        areaDF, // 已有商圈库数据
        geoHashDF.col("geoHash") === areaDF.col("geo_hash"), // 指定关联字段
        "left" // 左连接
      ) // 关联以后的字段：geoHash, longitude, latitude, area(当商圈库中不存商圈信息时，area为空
        // 获取未得到商圈信息的数据
        .filter($"area".isNull)
        // 6、自定义UDF，传递经纬度，调用高德API，获取商圈信息
        .select(
        $"geoHash".as("geo_hash"),
        get_area($"longitude", $"latitude").as("area") // 将经纬度传递到udf函数中
      )
      logger.warn(s"将过滤后去重后ODS数据去除AREA表中存在数据量：${businessAreaDF.count()}")
    } else {
      businessAreaDF = geoHashDF
        // 4、自定义UDF，传递经纬度，调用高德API，获取商圈信息
        .select(
        $"geoHash".as("geo_hash"),
        get_area($"longitude", $"latitude").as("area") // 将经纬度传递到udf函数中
      )
    }

    // 第六步、返回新的获取商圈信息
    businessAreaDF
  }



  /**
    * 开发步骤如下：
    * 1、获取GeoHash值、经度和维度
    * 2、依据经纬度范围，过滤非大陆的数据
    * 3、按照GeoHash去重（使用group by），获取组内经纬度平均值
    * 4、自定义UDF，传递经纬度，调用高德API，获取商圈信息
    * 5、保存商圈信息到Kudu表（GeoHash，Area）
    */
  def processDataV1(odsDF: DataFrame): DataFrame = {
    // TODO：到如隐式转换和函数库
    import odsDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // 注册UDF函数，传递经纬度获取商圈信息
    val get_area: UserDefinedFunction = udf(HttpUtils.loadGaode2Area _) // 当传递函数名称到高阶函数中时，需求使用空格下划线

    // 通过调用Dataset API构建商圈信息
    val businessAreaDF: DataFrame = odsDF
      // 1、获取GeoHash值、经度和维度
      .select($"geoHash", $"longitude", $"latitude")
      // 2、依据经纬度范围，过滤非大陆的数据, 经度73.66~135.05，纬度3.86~53.55
      .filter(
      $"longitude".geq(73.66).and($"longitude".leq(135.05))
        .and($"latitude".geq(3.86)).and($"latitude".leq(53.55))
    )
      // 3、按照GeoHash去重（使用group by），获取组内经纬度平均值
      .groupBy($"geoHash")
      .agg(
        round(avg($"longitude"), 6).as("longitude"), // 同一个GeoHash对应的所有的经度平均值
        round(avg($"latitude"), 6).as("latitude") // 同一个GeoHash对应的所有的维度平均值
      )
      // 4、自定义UDF，传递经纬度，调用高德API，获取商圈信息
      .select(
      $"geoHash".as("geo_hash"),
      get_area($"longitude", $"latitude").as("area") // 将经纬度传递到udf函数中
    )

    // 返回商圈信息
    businessAreaDF
  }
}
