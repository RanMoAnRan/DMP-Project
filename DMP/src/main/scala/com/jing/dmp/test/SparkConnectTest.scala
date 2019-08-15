package com.jing.dmp.test

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark GraphX构建图，获取其中连通的子图，用以识别统一用户
  */
object SparkConnectTest {
  def main(args: Array[String]): Unit = {
    // TODO: 1、构建SparkContext实例对象
    val sparkContext: SparkContext = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[4]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    )
    // 标签表中的获取的每条数据：main_id和ids
    val usersRDD: RDD[(String, Map[String, String])] = sparkContext.parallelize(List(
      ("M001", Map(("imei", "IM001"), ("mac", "MA001"), ("openudid", "OU001"), ("idfa", "ID001"))),
      ("M002", Map(("mac", "MA002"), ("openudid", "OU001"))),
      ("M003", Map(("imei", "IM003"), ("androidid", "AN003"), ("idfa", "ID001"))),
      ("M004", Map(("imei", "IM004"), ("mac", "MA004"), ("openudid", "OU004"))),
      ("M005", Map(("androidid", "AN005"), ("idfa", "ID005")))))

    // TODO: 2、图中顶点数据, 顶点属性为
    val idVerticesRDD: RDD[(VertexId, String)] = usersRDD.flatMap { case (mainId, idmaps) =>
      var list = new ListBuffer[(VertexId, String)]()
      // 主标识ID
      list += mainId.hashCode.toLong -> s"mainid->$mainId"
      // 其他标识ID
      idmaps.foreach { case (idKey, idValue) =>
        val vertexAttr = s"$idKey->$idValue"
        list += vertexAttr.hashCode.toLong -> vertexAttr
      }
      println(s"count = ${list.size}, str = ${list.mkString("$$")}")
      list.toList
    }.distinct // 将重复顶点去除

    idVerticesRDD.count()

    // TODO: 3、图中边的数据
    val idEdgesRDD: RDD[Edge[String]] = usersRDD.flatMap { case (mainId, idsMap) =>
      // 边的源端顶点（mainId主标识）
      val srcId: VertexId = mainId.hashCode.toLong
      // 标签库中每条数据构建出对应的边（主标识ID到所有标识ID）
      idsMap.map { case (idKey, idValue) =>
        // 边的目的端顶点
        val destId: VertexId = s"$idKey->$idValue".hashCode.toLong
        // 返回边
        Edge(srcId, destId, attr = "")
      }
    }

    // TODO: 4、使用顶点和边，构建图
    val graph: Graph[String, String] = Graph(idVerticesRDD, idEdgesRDD)
    // 将图数据缓存
    graph.cache()
    println(s"顶点的数目：${graph.vertices.count()}")
    println(s"边的数目：${graph.edges.count()}")

    // TODO: 5、从图中获取连通图
    // 返回连通图中顶点的属性值为：所在连通图中所有顶点VertexId最小的值
    val connectedGraph: Graph[VertexId, String] = graph.connectedComponents()
    // 获取图中所有的顶点，打印各个顶点所属的连通图
    connectedGraph.vertices.foreach { case (vertexId, ccVertexId) =>
      println(s"vertexId = $vertexId, ccVertexId = $ccVertexId")
    }

    connectedGraph.vertices
      // 与顶点RDD进行关联（依据VertexId），获取每个顶点属性值
      .join(idVerticesRDD)
      /*value.foreach{case (vertexId, (ccVertexId, vertexAttr)) =>
      println(s"$vertexId, $ccVertexId, $vertexAttr")}*/
      // 将连通图中最小VertexId作为Key，分组聚合获取每个连通图中所有顶点VertexId
      .map { case (vertexId, (ccVertexId, vertexAttr)) =>
      (ccVertexId, (vertexId, vertexAttr))
    } // 按照连通图中最小VertexId分组，获取连通图中所有VertexId集合
      .groupByKey()
      // 过滤找出每个连通图中主标识符ID
      .values
      .map { iter =>
        println(iter)
        iter.filter { case (_, vertexAttr)
        => vertexAttr.startsWith("mainid")
        }
          .map(tuple => tuple._2)
          .reduce((mainIdAttr1, mainIdAttr2) =>
        s"$mainIdAttr1, $mainIdAttr2")
      }
      .foreach(println)

    // 计算结束，释放缓存空间
    graph.unpersist()
    // 应用结束，关闭资源
    Thread.sleep(1000000)
    sparkContext.stop()

  }
}
