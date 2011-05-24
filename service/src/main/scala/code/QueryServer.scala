package code

import java.io.{ OutputStreamWriter, CharArrayWriter, Writer, OutputStream }
import java.sql._
import java.util.GregorianCalendar
import net.liftweb.common._
import net.liftweb.http.rest._
import net.liftweb.http._
import net.liftweb._
import scala.math.BigDecimal
import uniso.query.metadata.JDBCMetaData
import uniso.query.result._
import uniso.query._

/**
 * Serves query results as json. Supports HTTP GET and POST methods to /query
 * url. There is one required request parameter - query. It's value should
 * contain query expression. If this query expression has parameters,
 * more request parameters should be added. Leading "p_" is stripped from
 * parameter names before sending parameters to query core. Also, before sending
 * to query core, parameter values are type-converted. Supported types are
 * number, boolean (true/false), null, date (yyyy-mm-dd),
 * date-time (yyyy-mm-dd hh:mm:ss), string. Strings must be prefixed with '
 * (apostrophe), which will be stripped. You can turn off type convertion by
 * adding argtypes=string to request parameters. Then all query parameters will
 * be sent to query core as strings without modifications.
 */
object QueryServer extends RestHelper {

  val P_ = "p_" // pars prefix to legalize name
  val Plen = P_.size
  private var initialized = false

  object ArgTypes {
    val strong = "strong"
    val string = "string"
  }

  serve {
    case JsonGet("query" :: Nil, _) =>
      // FIXME served with GET - ensure query is readonly!!!
      respond
    case JsonPost("query" :: Nil, _) =>
      respond
    case Post("query" :: Nil, _) =>
      respond
  }

  def respond = {
    for {
      query <- S.param("query") ?~ "query is missing"
      req <- S.request ?~ "request is missing :-O"
    } yield {
      val argTypes = S.param("argtypes")
      val pars = (req.params - "query" - "argtypes").map(x =>
        (if (x._1 startsWith P_) x._1.substring(Plen) else x._1, x._2.head))
      OutputStreamResponse( //
        (os: OutputStream) => json(query, typeConvert(pars, argTypes), os),
        List("Content-Type" -> "application/json"));
    }
  }

  def typeConvert(pars: Map[String, String]): Map[String, Any] = {
    typeConvert(pars, Empty)
  }

  def typeConvert(pars: Map[String, String],
    argTypes: Box[String]): Map[String, Any] = {
    pars.map(x => (x._1, typeConvert(x._1, x._2, argTypes)))
  }

  def typeConvert(name: String, value: String, argTypes: Box[String]) = {
    argTypes match {
      case Empty => convert(name, value)
      //case Full(ArgTypes.duck) => convert(name, value, false)
      case Full(ArgTypes.strong) => convert(name, value)
      case Full(ArgTypes.string) => value
      case f: Failure => argTypes
    }
  }

  def convert(name: String, value: String, strong: Boolean = true) = {
    val DateP = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
    val DateTimeP =
      """(\d\d\d\d)\-(\d\d)\-(\d\d)[T ]?(\d\d)\:(\d\d)\:(\d\d)""".r
    val DecimalP = """(-)?(\d+)(\.\d*)?""".r
    val StringP = """\'(.*)""".r
    value match {
      case "" => null
      case "null" => null
      case "true" => true
      case "false" => false
      case DateTimeP(y, mo, d, h, mi, s) =>
        new GregorianCalendar(
          y.toInt, mo.toInt - 1, d.toInt, h.toInt, mi.toInt, s.toInt).getTime()
      case DateP(y, m, d) =>
        new GregorianCalendar(y.toInt, m.toInt - 1, d.toInt).getTime()
      case DecimalP(_, _, _) => BigDecimal(value)
      case StringP(s) => s
      case s if strong => Failure(
        "For argtypes=strong, strings must be" +
          " prefixed with ' (for " + name + "=" + value + ")", Empty, Empty)
      case s => s
    }
  }

  def json(expr: String, pars: Map[String, Any]): String = {
    val writer = new CharArrayWriter
    json(expr, pars, writer)
    writer.toString
  }

  def json(expr: String, pars: Map[String, Any], os: OutputStream) {
    val writer = new OutputStreamWriter(os, "UTF-8")
    json(expr, pars, writer)
    writer.flush
  }

  def json(expr: String, pars: Map[String, Any], writer: Writer) {
    json(
      System.getProperty(Conn.driverProp),
      System.getProperty(Conn.usrProp),
      System.getProperty(Conn.schemaProp),
      expr, pars, writer)
  }

  private def json(jdbcDriverClass: String, user: String, schema: String, //
    expr: String, pars: Map[String, Any], writer: Writer) {
    // Mulkibas te notiek. Ja jau core satur init kodu, tad kapec ne lidz galam?
    // TODO Kapec man janorada usr, bet nav janorada pwd?
    // TODO Kapec man jarupejas par driver class iekrausanu?
    if (!initialized) {
      if (jdbcDriverClass != null) Class.forName(jdbcDriverClass)
      Env update JDBCMetaData(user, schema)
      initialized = true;
    }
    val conn = Conn()()
    try {
      Env update conn
      Jsonizer.jsonize(Query(expr, pars), writer)
    } finally {
      conn close
    }
  }

  def bindVariables(expr: String): List[String] = { //
    QueryParser.bindVariables(expr)
  }
}
