package com.jing.dmp.utils

import java.time.Duration

import com.jing.dmp.config.AppConfigHelper
import okhttp3.{OkHttpClient, Request, Response}
import org.json4s.{JValue, JsonAST}
import org.json4s.jackson.JsonMethods.parse

/**
  * 使用OkHttpClient请求高德开发API，传递经纬度，获取商圈信息
  */
object HttpUtils {

  /**
    * 创建OkHttpClient
    */
  private val httpClient = new OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(20L)).build()


  /**
    * 根据经纬度 获取高德地图响应的商圈信息
    *
    * @param longitude 经度
    * @param latitude  纬度
    * @return JSON格式的商圈信息
    */
  def getLocationInfo(longitude: Double, latitude: Double): Option[String] = {
    // a. 拼接获取请求URL
    val regeoUrl: String = s"${AppConfigHelper.AMAP_URL}$longitude,$latitude"

    try {
      // b. 构建Request请求对象
      val request: Request = new Request.Builder()
        .url(regeoUrl) // 设置请求的URL
        .get() // 使用GET方式请求
        .build()

      // c. 通过HttpClient发送请求
      val response: Response = httpClient.newCall(request).execute()

      // d. 当响应状态码为200，获取响应的内容
      if (response.isSuccessful) {
        val respJson = response.body().string()
        // 将请求响应的内容封装到Some中
        Some(respJson)
      } else {
        None
      }
    } catch {
      case e: Exception => e.printStackTrace(); None
    }
  }


  /**
    * 解析高德地图返回的JSON,返回格式为 商圈1:商圈2:商圈3
    *
    * @param gaodeJson 请求高德获取JSON格式数据
    * @return
    */
  def parseJson(gaodeJson: String): String = {

    // 1. 通过parse函数解析Json数据
    val jRootValue: JValue = parse(gaodeJson)

    // 2. 依据节点名称businessAreas查找数据
    val jValue: JValue = jRootValue.\\("businessAreas")

    // 3. 获取子节点数据
    val children: Seq[JsonAST.JValue] = jValue.children

    // 4. 遍历子节点，通过名称检索name子节点获取值
    var businessArea = ""
    for (child <- children) {
      val value = child.\("name").values
      businessArea += s"$value:"
    }
    businessArea = businessArea.stripSuffix(":")

    // 返回解析获取的商圈信息
    businessArea
  }


  /**
    * 根据经纬度，请求高德，返回商圈信息
    *
    * @param longitude 经度
    * @param latitude  维度
    * @return 商圈信息
    */
  def loadGaode2Area(longitude: Double, latitude: Double): String = {
    // 第一步、发起网络请求，响应获取JSON数据
    val jsonOption: Option[String] = getLocationInfo(longitude, latitude)

    // 第二步、如果获取JSON，进行解析
    jsonOption match {
      case Some(gaodeJson) => parseJson(gaodeJson)
      case None => ""
    }
  }


  def main(args: Array[String]): Unit = {
    println(loadGaode2Area(104.06671, 30.666702))
  }

}
