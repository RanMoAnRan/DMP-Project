package com.jing.dmp.test

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于Spark GraphX构建图，包含定点（Vertex）和边（Edge）组成以及属性（Property）
  */
object SparkGraphTest {
  def main(args: Array[String]): Unit = {
    // TODO: 1、构建SparkContext实例对象
    val sparkContext: SparkContext = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("local[4]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    )


    // TODO: 2、图中顶点数据, 顶点ID为VertexId类型就是Long的别称，顶点属性为二元 组
    val userVertices: RDD[(VertexId, (String, String))] = sparkContext.parallelize(
      List(
        (3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))
      )
    )


    // TODO: 3、图中边的数据，边由两个顶点组成及边的属性（类型为String）
    /*
     case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
     var srcId: VertexId = 0,
     var dstId: VertexId = 0,
     var attr: ED = null.asInstanceOf[ED])
     */
    var relationshipEdges: RDD[Edge[String]] = sparkContext.parallelize(
      List(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")
      )
    )

    // TODO: 4、使用顶点和边，构建图
    // 默认的顶点属性值
    val defaultUserAttr: (String, String) = ("John Doe", "Missing")
    // 调用Graph对象object中apply方法获取图GrapImpl实例，得到图
    val graph: Graph[(String, String), String] = Graph(userVertices, relationshipEdges)

    // 将图数据缓存
    graph.cache()
    println("======================所有顶点的数据 ===========================")
    // TODO: 5、获取图中所有顶点数据
    graph.vertices.foreach { case (id, (name, attr)) => {
      println(s"$id: $name -> $attr")
    }
    }

    println("======================所有边的数据 ===========================")
    // TODO： 6、获取途中所有边的数据
    graph.edges.foreach { case Edge(srcId, dstId, attr) => {
      println(s"边：$srcId -> $dstId，属性值为：$attr")
    }
    }

    // TODO: 7、获取图中顶点属性为教授（prof）用户数
    var profCount: VertexId = graph.vertices.filter {
      case (id, (name, attr)) => {
        "prof".equals(attr)
      }
    }.count()
    println(s"教授（prof）的用户数目为：$profCount")

    // TODO: 8、两个顶点与边组成一个实体EdgeTriplet
    // class EdgeTriplet[VD, ED] extends Edge[ED]
    graph.triplets.foreach {
      triplet =>
        println(s"源端：${triplet.srcAttr} -> 终端：${triplet.dstAttr}，边 的属性：${triplet.attr}")
        // 获取EdgeTriplet转存为三元组信息
        println(triplet.toTuple.toString())
    }

    // 释放缓存数据
    graph.unpersist()
    // 应用结束，关闭资源
    sparkContext.stop()

  }

}
