package com.jing.dmp.test

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * 解析请求高度地图获取的JSON数据，获取商圈信息
  */
object Json4sTest {

	def main(args: Array[String]): Unit = {

		// a. 请求高度地图获取的JSON数据
		val regeoJson: String =
			"""
			  |{
			  |    "status":"1",
			  |    "regeocode":{
			  |        "addressComponent":{
			  |            "city":[
			  |
			  |            ],
			  |            "province":"北京市",
			  |            "adcode":"110108",
			  |            "district":"海淀区",
			  |            "towncode":"110108015000",
			  |            "streetNumber":{
			  |                "number":"5号",
			  |                "location":"116.310454,39.9927339",
			  |                "direction":"东北",
			  |                "distance":"94.5489",
			  |                "street":"颐和园路"
			  |            },
			  |            "country":"中国",
			  |            "township":"燕园街道",
			  |            "businessAreas":[
			  |                {
			  |                    "location":"116.303364,39.97641",
			  |                    "name":"万泉河",
			  |                    "id":"110108"
			  |                },
			  |                {
			  |                    "location":"116.314222,39.98249",
			  |                    "name":"中关村",
			  |                    "id":"110108"
			  |                },
			  |                {
			  |                    "location":"116.294214,39.99685",
			  |                    "name":"西苑",
			  |                    "id":"110108"
			  |                }
			  |            ],
			  |            "building":{
			  |                "name":"北京大学",
			  |                "type":"科教文化服务;学校;高等院校"
			  |            },
			  |            "neighborhood":{
			  |                "name":"北京大学",
			  |                "type":"科教文化服务;学校;高等院校"
			  |            },
			  |            "citycode":"010"
			  |        },
			  |        "formatted_address":"北京市海淀区燕园街道北京大学"
			  |    },
			  |    "info":"OK",
			  |    "infocode":"10000"
			  |}
			""".stripMargin

		// 1. 通过parse函数解析Json数据
		val jRootValue: JValue = parse(regeoJson)

		// 2. 依据节点名称businessAreas查找数据
		val jValue: JValue = jRootValue.\\("businessAreas")

		// 3. 获取子节点数据
		val children: Seq[JsonAST.JValue] = jValue.children

		// 4. 遍历子节点，通过名称检索name子节点获取值
		var businessArea = ""
		for(child <- children){
			val value = child.\("name").values
			println(value)
			businessArea += s"$value:"
		}
		businessArea = businessArea.stripSuffix(":")

		println(businessArea)
	}

}
