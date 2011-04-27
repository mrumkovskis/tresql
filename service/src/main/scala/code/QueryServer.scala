package code

import java.io.{ OutputStreamWriter, CharArrayWriter, Writer, OutputStream }
import net.liftweb.http.rest._
import net.liftweb.http._
import net.liftweb._
import uniso.query.result._
import uniso.query._

object QueryServer extends RestHelper {

  serve {
    // FIXME served with GET - ensure query is readonly!!!
    case JsonGet("query" :: Nil, _) =>
      for {
        query <- S.param("query") ?~ "query is missing" ~> 400
      } yield {
        OutputStreamResponse( //
          (os: OutputStream) => json(query, S.params("p"), os),
          List("Content-Type" -> "application/json"));
      }
  }

  def json(expr: String, pars: List[String]): String = {
    val writer = new CharArrayWriter
    json(expr, pars, writer)
    writer.toString
  }

  def json(expr: String, pars: List[String], os: OutputStream) {
    val writer = new OutputStreamWriter(os, "UTF-8")
    json(expr, pars, writer)
    writer.flush
  }

  def json(expr: String, pars: List[String], writer: Writer) { //
    json("com.sap.dbtech.jdbc.DriverSapDB", "burvis", "burfull2", expr, pars, writer)
  }

  def json(jdbcDriverClass: String, user: String, password: String, //
    expr: String, pars: List[String], writer: Writer) {
    Class.forName(jdbcDriverClass)
    val md = metadata.JDBCMetaData(user, password)
    val conn = Conn()()
    val env = new Env(Map(), md, conn)
    Env.metaData(metadata.JDBCMetaData(user, password))
    Jsonizer.jsonize(Query(expr, pars)(conn), writer)
    conn.close
  }
}
