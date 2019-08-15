package com.jing.dmp.beans

// 封装IP地址解析的值
case class IpRegion(
                     ip: String, //IP
                     longitude: Double, latitude: Double, //经度
                     province: String, city: String, //维度
                     geoHash: String //经纬度生成GeoHash值
                   )