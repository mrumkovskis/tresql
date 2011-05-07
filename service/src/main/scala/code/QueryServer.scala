package code

import java.io.{ OutputStreamWriter, CharArrayWriter, Writer, OutputStream }
import net.liftweb.http.rest._
import net.liftweb.http._
import net.liftweb._
import uniso.query.metadata.JDBCMetaData
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
    json(
      System.getProperty(Conn.driverProp),
      System.getProperty(Conn.usrProp),
      System.getProperty(Conn.schemaProp),
      expr, pars, writer)
  }

  def json(jdbcDriverClass: String, user: String, schema: String, //
    expr: String, pars: List[String], writer: Writer) {
    // Mulkibas te notiek. Ja jau core satur init kodu, tad kapec ne lidz galam?
    // TODO Kapec man janorada usr, bet nav janorada pwd?
    // TODO Kapec man jarupejas par driver class iekrausanu?
    if (jdbcDriverClass != null) Class.forName(jdbcDriverClass)
    Env update JDBCMetaData(user, schema)
    val conn = Conn()()
    Env update conn
    Jsonizer.jsonize(Query(expr, pars), writer)
    conn close
  }
}
