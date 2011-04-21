package code

import uniso.query.result._
import uniso.query._

object QueryServer {
  def json(expr: String, pars: List[String]): String = //
    json("com.sap.dbtech.jdbc.DriverSapDB", "burvis", "burfull2", expr, pars)

  def json(jdbcDriverClass: String, user: String, password: String, //
    expr: String, pars: List[String]): String = {
    Class.forName(jdbcDriverClass)
    val md = metadata.JDBCMetaData(user, password)
    val conn = Conn()()
    val env = new Env(Map(), md, conn)
    Env.metaData(metadata.JDBCMetaData(user, password))
    val writer = new java.io.CharArrayWriter
    Jsonizer.jsonize(Query(expr, pars)(conn), writer)
    writer.toString
  }
}
