package com.jing.dmp.test

import ch.hsr.geohash.GeoHash
import com.jing.dmp.config.AppConfigHelper
import com.maxmind.geoip.LookupService
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  * 使用ip2region来转换IP地址为省市，GeoLite来将IP地址转换为转换经纬度
  */
object IpConvertTest {

	/**
	  * IP -> Region（省份）、City（城市）
	  *
	  * @param ip IP地址
	  */
	def ip2Region(ip: String): Unit = {

		// a. 构建DbSearcher实例对象，传递数据文件地址
		val dbSearcher: DbSearcher = new DbSearcher(
			new DbConfig(), AppConfigHelper.IPS_DATA_REGION_PATH
		)

		// b. 解析IP地址为省份和城市 字符串：中国|0|安徽省|滁州市|电信
		val region = dbSearcher.btreeSearch(ip).getRegion
		println(region)

		// c. 获取省份和城市信息
		val Array(_, _, province, city, _) = region.trim.split("\\|")
		println(s"省份 = $province, 城市 = $city")
	}

	/**
	  *  IP -> 经度和维度
	  *
	  * @param ip IP地址
	  */
	def ip2Location(ip: String): Unit = {

		// a. 构建对象，加载数据文件
		val lookupService: LookupService = new LookupService(
			AppConfigHelper.IPS_DATA_GEO_PATH, LookupService.GEOIP_MEMORY_CACHE
		)

		// b. 解析IP地址得到Region信息
		val location = lookupService.getLocation(ip)

		// c. 获取经纬度数据
		println(s"经度 = ${location.longitude}, 维度 = ${location.latitude}, 省份 = ${location.region}, 城市 = ${location.city}")

		// d. 将经纬度转换为GeoHash值（字符串）
		val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(
			location.latitude, location.longitude, 8
		)
		println(s"GeoHash = $geoHash") // wm7b0ttr
	}

	def main(args: Array[String]): Unit = {
		val ip = "121.76.98.134"  // "36.62.163.115" // "106.87.131.39"
		// IP -> Region（省份）、City（城市）
		ip2Region(ip)
		// IP -> 经度和维度
		ip2Location(ip)
	}

}
