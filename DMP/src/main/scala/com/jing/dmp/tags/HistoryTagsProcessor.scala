package com.jing.dmp.tags

import com.jing.dmp.beans.{IdsWithTags, UserTags}
import com.jing.dmp.config.AppConfigHelper
import com.jing.dmp.process.Processor
import com.jing.dmp.utils.TagsUtils
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * 1. 读取历史表数据
  * 2. 如果获取到数据,进行标签衰减
  */
object HistoryTagsProcessor extends Processor {

  // 标签衰减系数
  private val TAG_COEFFICIENT: Double = AppConfigHelper.TAG_COEFF

  override def processData(historyTasDF: DataFrame): DataFrame = {

    import historyTasDF.sparkSession.implicits._

    val coeffTagsDS: Dataset[IdsWithTags] = historyTasDF
      // 为了操作结合准确性和安全性，将DataFrame转换为Dataset进行操作
      .as[UserTags]
      // 对每个分区数据进行操作
      .mapPartitions { datas =>
      datas.map {
        case UserTags(mainId, idsStr, tagsStr) =>

        // a. 将标识符ID字符串转换为Map
        val idsMap: Map[String, String] = TagsUtils.idsStr2Map(idsStr)

        // b. 将标签字符串转换为Map
        val tagsMap: Map[String, Double] = TagsUtils.tagsStr2Map(tagsStr)

        // c. 标签衰减，修改历史标签中各个标签的权重
        val coefficientTagsMap: Map[String, Double] = tagsMap
          .map {
          case (tagName, tagWeight) =>
          tagName -> tagWeight * TAG_COEFFICIENT
        }

        // d. 返回
        IdsWithTags(mainId, idsMap, coefficientTagsMap)
      }
    }

    // 返回历史标签DataFrame
    coeffTagsDS.map {
      case IdsWithTags(mainId, idsMap, tagsMap) =>
        UserTags(mainId, TagsUtils.map2Str(idsMap), TagsUtils.map2Str(tagsMap))
    }.toDF()
  }
}
