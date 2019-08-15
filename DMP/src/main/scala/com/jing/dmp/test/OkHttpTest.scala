package com.jing.dmp.test

import okhttp3.{OkHttpClient, Request, Response}
import java.time.Duration

/**
  * 依据经纬度和Key请求URL，获取返回JSON格式数据
  */
object OkHttpTest {

  // GaoDe 开发Key
  val GAODE_KEY = "7993ce7fce9ba2680322d42f315e7e0e"

  // 请求地址URL
  val REGEO_API_URL = s"https://restapi.amap.com/v3/geocode/regeo?" +
    s"location=116.310003,39.991957&key=$GAODE_KEY"

  def main(args: Array[String]): Unit = {
    // a. 构建OkHttpClient实例对象
   // val httpClient = new OkHttpClient()

    // 可以通过如下方式构建OkHttpClient对象，设置请求超时时间
    val httpClient = new OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(20L)).build()

    // b. 构建Request请求对象
    val request: Request = new Request.Builder()
      .url(REGEO_API_URL) // 设置请求的URL
      .get() // 使用GET方式请求
      .build()

    // c. 通过HttpClient发送请求
    val response: Response = httpClient.newCall(request).execute()

    // d. 当响应状态码为200，获取响应的内容
    if (200 == response.code()) {
      val respJson = response.body().string()
      println(respJson)
    }

  }

}
