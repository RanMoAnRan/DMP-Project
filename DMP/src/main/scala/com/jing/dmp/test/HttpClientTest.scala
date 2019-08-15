package com.jing.dmp.test

import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, HttpMethod}

/**
  * Apache开源的Http客户端HttpClient使用
  */
object HttpClientTest {

	// GaoDe 开发Key
	val GAODE_KEY = "7993ce7fce9ba2680322d42f315e7e0e"

	// 请求地址URL
	val REGEO_API_URL = s"https://restapi.amap.com/v3/geocode/regeo?" +
		s"location=116.310003,39.991957&key=$GAODE_KEY"


	def main(args: Array[String]): Unit = {

		// a. 构建HttpClient实例对象
		val client = new HttpClient()

		// b. 构建GET请求，传递URL
		val getMethod: HttpMethod = new GetMethod(REGEO_API_URL)
		// getMethod.setRequestHeader("", "")

		// c. 发送HTTP请求
		val status: Int = client.executeMethod(getMethod)

		// d. 判断响应状态码200，获取返回数据
		if(200 == status){
			val response: String = getMethod.getResponseBodyAsString
			// 打印
			println(response)
		}

	}

}
