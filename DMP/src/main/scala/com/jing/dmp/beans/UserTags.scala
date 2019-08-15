package com.jing.dmp.beans

/**
  * 落地到Kudu中的标签信息
  *
  * @param main_id 主iD
  * @param ids     ID集合字符串，如(k1:v1),(k2:v2)
  * @param tags    标签集合，如(t1:1.3),(t2:2.2)
  */
case class UserTags(main_id: String, ids: String, tags: String)