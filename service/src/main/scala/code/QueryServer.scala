package code

import java.io.{ OutputStreamWriter, CharArrayWriter, Writer, OutputStream }
import net.liftweb.http.rest._
import net.liftweb.http._
import net.liftweb._
import uniso.query.metadata.JDBCMetaData
import uniso.query.result._
import uniso.query._

object QueryServer extends RestHelper {

  val P_ = "p_" // pars prefix to legalize name
  val Plen = P_.size

  serve {
    // FIXME served with GET - ensure query is readonly!!!
    case JsonGet("query" :: Nil, _) =>
      for {
        query <- S.param("query") ?~ "query is missing"
        req <- S.request ?~ "request is missing :-O"
      } yield {
        val pars = req.params.map(x =>
          (if (x._1 startsWith P_) x._1.substring(Plen) else x._1, x._2.head))
        OutputStreamResponse( //
          (os: OutputStream) => json(query, pars, os),
          List("Content-Type" -> "application/json"));
      }
  }

  def json(expr: String, pars: Map[String, String]): String = {
    val writer = new CharArrayWriter
    json(expr, pars, writer)
    writer.toString
  }

  def json(expr: String, pars: Map[String, String], os: OutputStream) {
    val writer = new OutputStreamWriter(os, "UTF-8")
    json(expr, pars, writer)
    writer.flush
  }

  def json(expr: String, pars: Map[String, String], writer: Writer) { //
    json(
      System.getProperty(Conn.driverProp),
      System.getProperty(Conn.usrProp),
      System.getProperty(Conn.schemaProp),
      expr, pars, writer)
  }

  def json(jdbcDriverClass: String, user: String, schema: String, //
    expr: String, pars: Map[String, String], writer: Writer) {
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

  def bindVariables(expr: String): List[String] = { //
    QueryParser.bindVariables(expr)
  }
}
