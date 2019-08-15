package com.jing.dmp.tags

import java.util.UUID

import com.jing.dmp.beans.{IdsWithTags, UserTags}
import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.Processor
import com.jing.dmp.utils.TagsUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

/**
  * 今日ODS表和AREA表生成今日用户标签
  */
object MakeTagsProcessor extends Processor {

  /**
    * 生成标签数据：广告标识、渠道、关键词、省市、性别、年龄、商圈、App名称和设备
    */
  override def processData(odsDF: DataFrame): DataFrame = {
    val spark: SparkSession = odsDF.sparkSession
    val sc: SparkContext = spark.sparkContext
    // 导入隐式转换和函数库
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 获取字典信息并且广播变量广播出去
    val appDicMap: collection.Map[String, String] = sc
      .textFile("datas/dic_app.data") // 最好过滤不合格数据
      .map { line =>
      val Array(appId, appName) = line.split("##")
      (appId, appName)
    }
      .collectAsMap()
    val appDicMapBroadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(appDicMap)

    // 获取设备相关字典信息并且通过广播变量广播到Executor上
    val deviceDicMap: collection.Map[String, String] = sc
      .textFile("datas/dic_device.data")
      .map { line =>
        val Array(appId, appName) = line.split("##")
        (appId, appName)
      }
      .collectAsMap()
    val deviceDicMapBroadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(deviceDicMap)

    /** 遍历用户的ods信息,经过表连接此时已经有了商圈信息
      * a. 提取各个标签属性值，求得标签集合
      * b. 获取id集合
      * c. 获取主ID
      * d. 返回样例类 */
    val tagsDS: Dataset[IdsWithTags] = odsDF.mapPartitions { datas =>
      datas.map { row =>
        // 构建集合Map对象，用于存放标签数据
        var tagsMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

        // a. 广告类型标签（AdTypeTag）
        tagsMap ++= AdTypeTag.make(row)
        // b. 渠道标签（ChannelTag）
        tagsMap ++= ChannelTagMaker.make(row)
        // c. 关键词标签（KeywordTag)
        tagsMap ++= KeywordTagMaker.make(row)
        // d. 省份标签（ProvinceTag）
        tagsMap ++= ProvinceTagMaker.make(row)
        // e. 城市标签（CityTag）
        tagsMap ++= CityTagMaker.make(row)
        // f. 性别标签（GenderTag）
        tagsMap ++= GenderTagMaker.make(row)
        // g. 年龄标签（AgeTag）
        tagsMap ++= AgeTagMaker.make(row)
        // h. 商圈标签（AreaTag）
        tagsMap ++= AreaTagMaker.make(row)


        // a. App名称标签（AppNameTag）
        tagsMap ++= AppNameTagMaker.make(row, appDicMapBroadcast.value.toMap)
        // b. 设备类型标签（DeviceClientTag）
        tagsMap ++= DeviceClientTagMaker.make(row, deviceDicMapBroadcast.value.toMap)
        // c. 联网方式名称标签（DeviceNetworkTag）
        tagsMap ++= DeviceNetworkTagMaker.make(row, deviceDicMapBroadcast.value.toMap)
        // d. 运营商名称标签（DeviceIspTag）
        tagsMap ++= DeviceIspTagMaker.make(row, deviceDicMapBroadcast.value.toMap)

        // 获取每条数据中标识符ID的值（Map集合）自定义方法获取
        val idsMap: Map[String, String] = getIds(row)

        // 每条数据主标识符ID
        val mainId = row.getAs[String]("uuid") //UUID.randomUUID().toString

        IdsWithTags(mainId, idsMap, tagsMap.toMap)
      }
    }

    // 针对Dataset中数据进行转换（map转换为string）
    val tagsDF: DataFrame = tagsDS.map {
      case IdsWithTags(mainId, idsMap, tagsMap) =>
        UserTags(mainId, TagsUtils.map2Str(idsMap), TagsUtils.map2Str(tagsMap))
    }.toDF()

    // 返回DataFrame（标签DataFrame）
    tagsDF
  }

  /**
    * 每一行数据，不一定所有的id都有值，根据id的名称来查询值，并且过滤掉不含值的数据
    *
    * @param row DataFrame中每行数据
    */
  def getIds(row: Row): Map[String, String] = {
    //定义一个可变map集合存储数据
    var idsMap: mutable.Map[String, String] = mutable.Map[String, String]()

    // a. 获取所有ID标识符名称(15个标识符ID的名称）
    val ids: Array[String] = AppConfigHelper.ID_FIELDS.split(",")

    // b. 循环遍历
    for (id <- ids) {
      val idValue = row.getAs[String](id)
      // 添加到集合中
      idsMap += id -> idValue
    }

    // c. 过滤空值
    idsMap.toMap.filter { case (idName, idValue) => StringUtils.isNotEmpty(idValue) }
  }
}
