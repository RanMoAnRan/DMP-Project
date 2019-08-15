package com.jing.dmp.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间工具类
  */
object DateUtils {

  /**
    * 获取当前的日期，格式为:yyyyMMdd
    * 使用lang3中FastDateFormat格式化日期格式
    * 由于JDK里自带的SimpleDateFormat存在线程不安全问题
    */
  def getTodayDate(): String = {
    // a. 获取当前日期
    val nowDate = new Date()
    // b. 转换日期格式
    FastDateFormat.getInstance("yyyyMMdd").format(nowDate)
  }

  /**
    * 获取昨日的日期，格式为:20190710
    */
  def getYesterdayDate(): String = {
    // a. 获取Calendar对象
    val calendar = Calendar.getInstance()
    // b. 获取昨日日期
    calendar.add(Calendar.DATE, -1)
    // c. 转换日期格式
    FastDateFormat.getInstance("yyyyMMdd").format(calendar)
  }


}
