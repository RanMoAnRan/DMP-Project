package com.jing.dmp.report

/**
  * 统计报表的SQL语句
  */
object ReportSQLConstant {

	/**
	  * 广告投放的不同维度分布统计的SQL语句
	  *
	  * @param reportViewName DataFrame注册的临时视图名称
	  */
	def reportAdsKpiSQL(reportViewName: String, groupFields: Seq[String]): String = {
		// a. 将分组字段拼接为字符串，使用逗号分割
		val groupFiledsStr: String = groupFields.mkString(", ")

		// b.
		s"""
		   |WITH temp AS (
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  $groupFiledsStr ,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 1 THEN 1 ELSE 0
		   |	END
		   |  )AS orginal_req_cnt   ,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 2 THEN 1 ELSE 0
		   |	END
		   |  )AS valid_req_cnt     ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 1 AND processnode = 3 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_req_cnt        ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND isbid = 1 AND adorderid != 0
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS join_rtx_cnt      ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS success_rtx_cnt   ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_show_cnt       ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_click_cnt      ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_show_cnt    ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_click_cnt   ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN winprice / 1000 ELSE 0
		   |    END
		   |  )AS dsp_pay_money     ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN adpayment / 1000 ELSE 0
		   |    END
		   |  )AS dsp_cost_money
		   |FROM
		   |  $reportViewName
		   |GROUP BY
		   |  $groupFiledsStr
		   |)
		   |SELECT
		   |  report_date,
		   |  $groupFiledsStr,
		   |  orginal_req_cnt,
		   |  valid_req_cnt,
		   |  ad_req_cnt,
		   |  join_rtx_cnt,
		   |  success_rtx_cnt,
		   |  ad_show_cnt,
		   |  ad_click_cnt,
		   |  media_show_cnt,
		   |  media_click_cnt,
		   |  dsp_pay_money,
		   |  dsp_cost_money,
		   |  ROUND((success_rtx_cnt / join_rtx_cnt), 2) AS success_rtx_rate,
		   |  ROUND((ad_click_cnt / ad_show_cnt ), 2) AS ad_click_rate,
		   |  ROUND((media_click_cnt / media_show_cnt), 2) AS media_click_rate
		   |FROM
		   |  temp
		   |WHERE
		   |   join_rtx_cnt != 0 AND success_rtx_cnt != 0 AND
		   |   ad_show_cnt != 0 AND ad_click_cnt != 0 AND
		   |   media_show_cnt != 0 AND media_click_cnt != 0
		 """.stripMargin
	}

	/**
	  * 广告投放的地域分布的SQL语句
	  *
	  * @param tempViewName DataFrame注册的临时视图名称
	  */
	def reportAdsRegionSQL(tempViewName: String): String = {
		s"""
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  province, city,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 1 THEN 1 ELSE 0
		   |	END
		   |  )AS orginal_req_cnt   ,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 2 THEN 1 ELSE 0
		   |	END
		   |  )AS valid_req_cnt     ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 1 AND processnode = 3 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_req_cnt        ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND isbid = 1 AND adorderid != 0
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS join_rtx_cnt      ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS success_rtx_cnt   ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_show_cnt       ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_click_cnt      ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_show_cnt    ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_click_cnt   ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN winprice / 1000 ELSE 0
		   |    END
		   |  )AS dsp_pay_money     ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN adpayment / 1000 ELSE 0
		   |    END
		   |  )AS dsp_cost_money
		   |FROM
		   |  $tempViewName
		   |GROUP BY
		   |  province, city
		 """.stripMargin
	}

	/**
	  * 统计竞价成功率、广告点击率、媒体点击率的SQL
	  */
	def reportAdsRegionRateSQL(reportViewName: String): String = {
		s"""
		   |SELECT
		   |  t.report_date,
		   |  t.province,
		   |  t.city,
		   |  t.orginal_req_cnt,
		   |  t.valid_req_cnt,
		   |  t.ad_req_cnt,
		   |  t.join_rtx_cnt,
		   |  t.success_rtx_cnt,
		   |  t.ad_show_cnt,
		   |  t.ad_click_cnt,
		   |  t.media_show_cnt,
		   |  t.media_click_cnt,
		   |  t.dsp_pay_money,
		   |  t.dsp_cost_money,
		   |  ROUND((t.success_rtx_cnt / t.join_rtx_cnt), 2) AS success_rtx_rate,
		   |  ROUND((t.ad_click_cnt / t.ad_show_cnt ), 2) AS ad_click_rate,
		   |  ROUND((t.media_click_cnt / t.media_show_cnt), 2) AS media_click_rate
		   |FROM
		   |  $reportViewName t
		   |WHERE
		   |   t.join_rtx_cnt != 0 AND t.success_rtx_cnt != 0 AND
		   |   t.ad_show_cnt != 0 AND t.ad_click_cnt != 0 AND
		   |   t.media_show_cnt != 0 AND t.media_click_cnt != 0
		 """.stripMargin
	}



	/**
	  * 统计竞价成功率、广告点击率、媒体点击率的SQL
	  */
	def reportAdsRegionSQLSub(reportViewName: String): String = {
		s"""
		   |SELECT
		   |  t.report_date,
		   |  t.province,
		   |  t.city,
		   |  t.orginal_req_cnt,
		   |  t.valid_req_cnt,
		   |  t.ad_req_cnt,
		   |  t.join_rtx_cnt,
		   |  t.success_rtx_cnt,
		   |  t.ad_show_cnt,
		   |  t.ad_click_cnt,
		   |  t.media_show_cnt,
		   |  t.media_click_cnt,
		   |  t.dsp_pay_money,
		   |  t.dsp_cost_money,
		   |  ROUND((t.success_rtx_cnt / t.join_rtx_cnt), 2) AS success_rtx_rate,
		   |  ROUND((t.ad_click_cnt / t.ad_show_cnt ), 2) AS ad_click_rate,
		   |  ROUND((t.media_click_cnt / t.media_show_cnt), 2) AS media_click_rate
		   |FROM (
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  province, city,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 1 THEN 1 ELSE 0
		   |	END
		   |  )AS orginal_req_cnt   ,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 2 THEN 1 ELSE 0
		   |	END
		   |  )AS valid_req_cnt     ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 1 AND processnode = 3 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_req_cnt        ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND isbid = 1 AND adorderid != 0
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS join_rtx_cnt      ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS success_rtx_cnt   ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_show_cnt       ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_click_cnt      ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_show_cnt    ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_click_cnt   ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN winprice / 1000 ELSE 0
		   |    END
		   |  )AS dsp_pay_money     ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN adpayment / 1000 ELSE 0
		   |    END
		   |  )AS dsp_cost_money
		   |FROM
		   |  $reportViewName
		   |GROUP BY
		   |  province, city
		   |) t
		   |WHERE
		   |   t.join_rtx_cnt != 0 AND t.success_rtx_cnt != 0 AND
		   |   t.ad_show_cnt != 0 AND t.ad_click_cnt != 0 AND
		   |   t.media_show_cnt != 0 AND t.media_click_cnt != 0
		 """.stripMargin
	}


	/**
	  * 统计竞价成功率、广告点击率、媒体点击率的SQL
	  */
	def reportAdsRegionSQLWith(reportViewName: String): String = {
		s"""
		   |WITH temp AS (
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  province, city,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 1 THEN 1 ELSE 0
		   |	END
		   |  )AS orginal_req_cnt   ,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 2 THEN 1 ELSE 0
		   |	END
		   |  )AS valid_req_cnt     ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 1 AND processnode = 3 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_req_cnt        ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND isbid = 1 AND adorderid != 0
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS join_rtx_cnt      ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS success_rtx_cnt   ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_show_cnt       ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_click_cnt      ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_show_cnt    ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_click_cnt   ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN winprice / 1000 ELSE 0
		   |    END
		   |  )AS dsp_pay_money     ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN adpayment / 1000 ELSE 0
		   |    END
		   |  )AS dsp_cost_money
		   |FROM
		   |  $reportViewName
		   |GROUP BY
		   |  province, city
		   |)
		   |SELECT
		   |  t.report_date,
		   |  t.province,
		   |  t.city,
		   |  t.orginal_req_cnt,
		   |  t.valid_req_cnt,
		   |  t.ad_req_cnt,
		   |  t.join_rtx_cnt,
		   |  t.success_rtx_cnt,
		   |  t.ad_show_cnt,
		   |  t.ad_click_cnt,
		   |  t.media_show_cnt,
		   |  t.media_click_cnt,
		   |  t.dsp_pay_money,
		   |  t.dsp_cost_money,
		   |  ROUND((t.success_rtx_cnt / t.join_rtx_cnt), 2) AS success_rtx_rate,
		   |  ROUND((t.ad_click_cnt / t.ad_show_cnt ), 2) AS ad_click_rate,
		   |  ROUND((t.media_click_cnt / t.media_show_cnt), 2) AS media_click_rate
		   |FROM
		   |  temp t
		   |WHERE
		   |   t.join_rtx_cnt != 0 AND t.success_rtx_cnt != 0 AND
		   |   t.ad_show_cnt != 0 AND t.ad_click_cnt != 0 AND
		   |   t.media_show_cnt != 0 AND t.media_click_cnt != 0
		 """.stripMargin
	}

	/**
	  *
	  */
	def reportAdsAppSQLWith(reportViewName: String): String = {
		s"""
		   |WITH temp AS (
		   |SELECT
		   |  CAST(TO_DATE(NOW()) AS STRING) AS report_date,
		   |  appid, appname,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 1 THEN 1 ELSE 0
		   |	END
		   |  )AS orginal_req_cnt   ,
		   |  SUM(
		   |    CASE
		   |	  WHEN requestmode = 1 AND processnode >= 2 THEN 1 ELSE 0
		   |	END
		   |  )AS valid_req_cnt     ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 1 AND processnode = 3 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_req_cnt        ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND isbid = 1 AND adorderid != 0
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS join_rtx_cnt      ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1
		   |     THEN 1 ELSE 0
		   |    END
		   |  )AS success_rtx_cnt   ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_show_cnt       ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS ad_click_cnt      ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 2 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_show_cnt    ,
		   |  SUM(
		   |    CASE
		   |     WHEN requestmode = 3 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0
		   |    END
		   |  )AS media_click_cnt   ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN winprice / 1000 ELSE 0
		   |    END
		   |  )AS dsp_pay_money     ,
		   |  SUM(
		   |    CASE
		   |      WHEN adplatformproviderid >= 100000 AND iseffective = 1
		   |      AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND  adcreativeid > 200000
		   |     THEN adpayment / 1000 ELSE 0
		   |    END
		   |  )AS dsp_cost_money
		   |FROM
		   |  $reportViewName
		   |GROUP BY
		   |  appid, appname
		   |)
		   |SELECT
		   |  t.report_date,
		   |  t.appid,
		   |  t.appname,
		   |  t.orginal_req_cnt,
		   |  t.valid_req_cnt,
		   |  t.ad_req_cnt,
		   |  t.join_rtx_cnt,
		   |  t.success_rtx_cnt,
		   |  t.ad_show_cnt,
		   |  t.ad_click_cnt,
		   |  t.media_show_cnt,
		   |  t.media_click_cnt,
		   |  t.dsp_pay_money,
		   |  t.dsp_cost_money,
		   |  ROUND((t.success_rtx_cnt / t.join_rtx_cnt), 2) AS success_rtx_rate,
		   |  ROUND((t.ad_click_cnt / t.ad_show_cnt ), 2) AS ad_click_rate,
		   |  ROUND((t.media_click_cnt / t.media_show_cnt), 2) AS media_click_rate
		   |FROM
		   |  temp t
		   |WHERE
		   |   t.join_rtx_cnt != 0 AND t.success_rtx_cnt != 0 AND
		   |   t.ad_show_cnt != 0 AND t.ad_click_cnt != 0 AND
		   |   t.media_show_cnt != 0 AND t.media_click_cnt != 0
		 """.stripMargin
	}
}
