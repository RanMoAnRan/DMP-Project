import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object Jdbc {

  /**
    * 通过JDBC方式读取Impala表中的数据（映射关联Kudu中的表）
    */
  def main(args: Array[String]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null

    try{
      // 建立连接
      conn = DriverManager.getConnection("jdbc:impala://hadoop03:21050/default")

      // 准备SQL
      val querySQL = "select * from itcast_users limit 10"
      ps = conn.prepareStatement(querySQL)
      rs = ps.executeQuery()

      // 获取数据
      while (rs.next()) {
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val age = rs.getInt("age")
        println(id, name, age)
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(null != rs) rs.close()
      if(null != rs) ps.close()
      if(null != rs) conn.close()
    }
  }
}
