package com.jing.dmp.test

import ch.hsr.geohash.GeoHash
import com.maxmind.geoip.{Location, LookupService}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}


/**
  * 使用ip2region来转换IP地址为省市，GeoLite来将IP地址转换为转换经纬度
  */
object IpUtilsTest {
  /**
    * IP -> Region（省份）、City（城市）
    *
    * @param ip IP地址
    */
  def ip2Region(ip: String): Unit = {
    // 创建DbSearch实例对象
    val dbSearcher = new DbSearcher(new DbConfig(), "datas/ip2region.db")
    // 解析IP地址，获取Region值：中国|0|上海|上海市|有线通
    val region: String = dbSearcher.btreeSearch(ip).getRegion
    println(region)
    // 按照分隔符进行分割
    val Array(_, _, province, city, _) = region.split("\\|")
    println(s"province = $province, city = $city")
  }

  /**
    *  IP -> 经度和维度
    *
    * @param ip IP地址
    */
  def ip2Location(ip: String): Unit = {
    // a. 创建服务入口
    val service: LookupService = new LookupService(
      "datas/GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE
    )
    // b. 搜索
    val location: Location = service.getLocation(ip)
    // c. 获取经度和维度
    println(s"经度 = ${location.longitude}, 维度 = ${location.latitude}")

    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(location.latitude, location.longitude, 8)
    println(s"GeoHash = $geoHash")
  }

  def main(args: Array[String]): Unit = {
    val ip = "36.62.163.115" // "106.87.131.39"  // "121.76.98.134"
    // IP -> Region（省份）、City（城市）
    ip2Region(ip)
    // IP -> 经度和维度
    ip2Location(ip)
  }
}
