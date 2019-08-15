package com.jing.dmp.tags

import com.jing.dmp.beans.{IdsWithTags, UserTags}
import com.jing.dmp.process.Processor
import com.jing.dmp.utils.TagsUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
  * 标签的合并（今日ODS表中将数据标签与昨日用户标签进行合并，涉及统一用户识别）
  */
object MergeTagsProcessor extends Processor {

  override def processData(allTagsDF: DataFrame): DataFrame = {

    // 1、导入隐式转换和函数
    val spark: SparkSession = allTagsDF.sparkSession
    import allTagsDF.sparkSession.implicits._

    // 2、将合并后的数据标签的Dataset转换为RDD，进行操作（构建顶点RDD和边的RDD）
    val allTagsRDD: RDD[IdsWithTags] = allTagsDF.as[IdsWithTags].rdd

    // TODO: 3、构建图的顶点RDD数据
    /*
      顶点的相关信息：
        a. 顶点是主标识符mainId的话，Id -> mainId值的hashCode， Attr -> mainId#tags
        b. 顶点是标识符id的话, Id -> diName和idValue 组合的hashCode, Attr -> 组合的值
      */
    val idVerticesRDD: RDD[(Long, String)] = allTagsRDD.flatMap { case IdsWithTags(mainId, idsMap, tagsMap) =>
      // a. 创建一个列表，存储构建顶点信息
      val vertexList = new ListBuffer[(Long, String)]()

      // b. 创建 主标识MainId的顶点信息
      vertexList += mainId.hashCode.toLong -> s"mainId#$mainId#${TagsUtils.map2Str(tagsMap)}"

      // c. 对标识符IDS逐一创建顶点信息
      idsMap.map { case (idName, idValue) =>
        // 将标识符id和对应的值组合在一起
        val tagValue = s"$idName#$idValue"
        vertexList += tagValue.hashCode.toLong -> tagValue
      }

      // d. 返回list集合（全部构建顶点信息）
      vertexList.toList
    }.distinct() // 对顶点数据进行去重


    // TODO: 4、构建图的边RDD数据
    /*
      边的信息，此处边的属性 -> 默认值("group") , srcId为mainId，dst为各个标识符id
     */
    val idEdgesRDD: RDD[Edge[String]] = allTagsRDD.flatMap { case IdsWithTags(mainId, idsMap, _) =>
      // a. 获取srcId顶点
      val srcId = mainId.hashCode.toLong

      // b. 遍历ids标识符，逐一构建边
      val edgeMap = idsMap.map { case (idName, idValue) =>
        // 将标识符id和对应的值组合在一起
        val tagValue = s"$idName#$idValue"

        // b.1 构建dst顶点
        val dstId = tagValue.hashCode.toLong

        // b.2 构建边
        Edge(srcId, dstId, "default-edge-group")
      }

      // c. 返回
      edgeMap.toList
    }

    // TODO: 5、传递顶点RDD和图RDD构建图
    val dmpGrap: Graph[String, String] = Graph(idVerticesRDD, idEdgesRDD)
    println(s"图中顶点的个数 = ${dmpGrap.vertices.count()}")
    println(s"图中边的个数 = ${dmpGrap.edges.count()}")

    // TODO：6、获取连通图
    val dmpCcGraph: Graph[VertexId, String] = dmpGrap.connectedComponents()


    // TODO: 7、关联原图中顶点RDD，聚合每个连通图中标签 -> 用户标签，进行统一用户识别
    val userTagsRDD: RDD[UserTags] = dmpCcGraph
      // 获取连通图中所有顶点，此处每个顶点的属性为连通图中的标识符（连通图中所有顶点的最小值）
      .vertices
      // 关联原图的顶点RDD，获取连通图中各个顶点的属性
      .join(idVerticesRDD)
      // 将连通图的Id作为Key，以便分组，获取各个连通图中的所有顶点
      .map { case (vertexId, (ccVertexId, vertexAttr)) =>
      // 返回
      (ccVertexId, (vertexId, vertexAttr))
    }
      // 按照连通图中id分组，获取各个连通图的顶点数据
      .groupByKey()
      // 仅仅获取各个连通图中顶点数据，封装在迭代器中iter
      .values
      // 对连通图中的数据进行聚合标签，标签信息存储在mainid的顶点的属性中
      .map { iter =>
      // val xx: Iterable[(VertexId, String)] = iter
      // 打印显示，各个连通图中的顶点数目
      // println(s"顶点的个数 = ${iter.size}")

      // TODO： 由于多次使用迭代器中的数据，所以将迭代器数据存储到列表list中
      val verticesList: List[(VertexId, String)] = iter.toList

      // i. 获取主标识ID
      val mainId: String = {
        verticesList
          // 获取各个顶点的属性值
          .map { case (_, vertexAttr) => vertexAttr }
          // 过滤获取主标识的顶点
          .filter(vertexAttr => vertexAttr.startsWith("mainId#"))
          //
          .head.stripPrefix("mainId#").split("#")(0)
      }

      // ii. 获取连通图中所有除去主标识的ID
      val idsMap: Map[String, String] = {
        verticesList
          // 获取连通图中非主标识符ID
          .filter { case (_, vertexAttr) => !vertexAttr.startsWith("mainId#") }
          .map { case (_, vertexAttr) =>
            // 按照 # 分割顶点属性值，获取对应的标识符ID和值
            val Array(idName, idValue) = vertexAttr.split("#")
            // 以二元组返回
            idName -> idValue
          }
          .toMap
      } // 将列表List（二元组数据类型）的集合转换为Map

      // iii. 合并连通图中所有主标识符ID属性值中标签的值
      val ccTagsMap: String = {
        verticesList
          // 获取连通图中所有主标识ID
          .filter { case (_, vertexAttr) => vertexAttr.startsWith("mainId#") }
          // 获取属性值
          .map { case (_, vertexAttr) => vertexAttr.stripPrefix("mainId#").split("#")(1) }
          // 聚合操作
          .reduce { (tempTags, tags) =>
          // 其一、将标签转换为Map集合  (AD@插屏->1.0)
          val tempTagsMap: Map[String, Double] = TagsUtils.tagsStr2Map(tempTags)
          val tagsMap: Map[String, Double] = TagsUtils.tagsStr2Map(tags)

          // 其二、遍历聚合临时标签变量，判断是否包含Key
          val tempMap: Map[String, Double] = tempTagsMap.map { case (tagName, weight) =>
            if (tagsMap.contains(tagName)) {
              tagName -> (weight + tagsMap(tagName))
            } else {
              tagName -> weight
            }
          }

          // 两个集合Map合并，注意位置，转换为String
          TagsUtils.map2Str(tagsMap ++ tempMap)
        }
      }

      // case class UserTags(main_id: String, ids: String, tags: String)
      UserTags(mainId, TagsUtils.map2Str(idsMap), ccTagsMap)
    }

    // 将RDD转换为DataFrame，由于RDD中数据类型为CaseClass，可以通过反射字段推断数据类型
    userTagsRDD.toDF()
  }
}
