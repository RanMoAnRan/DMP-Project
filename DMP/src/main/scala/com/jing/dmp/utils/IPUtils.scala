package com.jing.dmp.utils

import ch.hsr.geohash.GeoHash
import com.jing.dmp.beans.IpRegion
import com.jing.dmp.config.AppConfigHelper
import com.maxmind.geoip.LookupService
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  * 一个解析IP地址的工具类：
  * 实现转换IP为样例类(经纬度、省市、IP、GeoHash)，封装到IpRegion类中。
  */
object IPUtils {

  /**
    * 转换IP地址为经纬度、省份城市及GeoHash值
    *
    * @param ip IP地址
    * @return
    */
  def convertIp2Region(ip: String, dbSearcher: DbSearcher, lookupService: LookupService): IpRegion = {

    // 1. 解析IP地址省份和城市
    val region = dbSearcher.btreeSearch(ip).getRegion
    val Array(_, _, province, city, _) = region.trim.split("\\|")

    // 2. 解析IP地址为经纬度
    val location = lookupService.getLocation(ip)
    val longitude = location.longitude.toDouble
    val latitude = location.latitude.toDouble

    // 3. 依据经纬度生成GeoHash值
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(
      location.latitude, location.longitude, 8
    )

    // 4. 返回值
    IpRegion(ip, longitude, latitude, province, city, geoHash)
  }


  /**
    * 转换IP地址为经纬度、省份城市及GeoHash值
    *
    * 由于调用此工具类每次都要new DbSearcher和new LookupService对象，因此使用上面的工具类（将两个对象传递过来）
    *
    * @param ip IP地址
    * @return
    */
  def convertIp2Region(ip: String): IpRegion = {

    // 1. 解析IP地址省份和城市
    val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), AppConfigHelper.IPS_DATA_REGION_PATH)

    val region = dbSearcher.btreeSearch(ip).getRegion
    val Array(_, _, province, city, _) = region.trim.split("\\|")

    // 2. 解析IP地址为经纬度
    val lookupService: LookupService = new LookupService(AppConfigHelper.IPS_DATA_GEO_PATH, LookupService.GEOIP_MEMORY_CACHE)
    val location = lookupService.getLocation(ip)
    val longitude = location.longitude.toDouble
    val latitude = location.latitude.toDouble

    // 3. 依据经纬度生成GeoHash值
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(
      location.latitude, location.longitude, 8
    )

    // 4. 返回值
    IpRegion(ip, longitude, latitude, province, city, geoHash)
  }

}
