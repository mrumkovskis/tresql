package code

import java.io.{ OutputStreamWriter, CharArrayWriter, Writer, OutputStream }
import java.sql._
import java.util.GregorianCalendar
import net.liftweb.common._
import net.liftweb.http.rest._
import net.liftweb.http._
import net.liftweb._
import scala.math.BigDecimal
import scala.collection.mutable.ConcurrentMap //can't import all mutable classes with (_) becauce code use immutable Map
import scala.collection.immutable.Map //
import scala.collection.JavaConversions._ //need for ConcurrentMap to incorporate with Java ConcurrentHashMap
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
 * be sent to query core as strings without modifications. You can modify result
 * by adding res-type to request parameters. Supported res-type values are
 * objects (default), arrays, object.
 */
object QueryServer extends RestHelper {

  val P_ = "p_" // pars prefix to legalize name
  val Plen = P_.size
  val mapForMetaData: ConcurrentMap[String, MetaData] = new java.util.concurrent.ConcurrentHashMap[String, MetaData]

  /** special parameter names */
  private object PN {
    val database = "database"
    val dbschema = "dbschema"
    val query = "query"
    val argtypes = "argtypes"
    val restype = "res-type"
  }

  object ArgTypes {
    val strong = "strong"
    val string = "string"
  }
  def defaultArgTypes = ArgTypes.strong

  object ResultType {
    val objects = "objects"
    val arrays = "arrays"
    val singleObject = "object"
  }
  def defaultResultType = Jsonizer.Objects

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
    import Jsonizer.ResultType
    for {
      database <- S.param(PN.database) match {
    	case Empty => Box[String]("")
    	case Full(s:String) => Box[String](s)
    	case f: Failure => new Failure(
    			"Failed to get database name", Empty, Box(f))
      }
      dbschema <- S.param(PN.dbschema) match {
  		case Empty => Box[String]("")
  		case Full(s:String) => Box[String](s)
  		case f: Failure => new Failure(
  	    	  "Failed to get database schema name", Empty, Box(f))
      }
      query <- S.param(PN.query) ?~ "query is missing"
      req <- S.request ?~ "request is missing :-O"
      resType <- S.param(PN.restype) match {
        case Empty => Box[ResultType](defaultResultType)
        case Full(s) => s match {
          case ResultType.objects => Box[ResultType](Jsonizer.Objects)
          case ResultType.singleObject => Box[ResultType](Jsonizer.Object)
          case ResultType.arrays => Box[ResultType](Jsonizer.Arrays)
          case s => Failure("Unknown result type value: " + s)
        }
        case f: Failure => new Failure(
          "Failed to get result type value", Empty, Box(f))
      }
      argTypes <- S.param(PN.argtypes) match {
        case Empty => Full(defaultArgTypes)
        case Full(s) if (s == ArgTypes.string || s == ArgTypes.strong) => Full(s)
        case Full(s) => Failure("Unknown argtypes value: " + s)
        case f: Failure => new Failure(
          "Failed to get result type parameter value", Empty, Box(f))
      }
    } yield {
      val pars = (req.params - PN.database - PN.dbschema - PN.query - PN.argtypes - PN.restype).map(x =>
        (if (x._1 startsWith P_) x._1.substring(Plen) else x._1, x._2.head))
      OutputStreamResponse( //
        (os: OutputStream) =>
          json(database, dbschema, query, typeConvert(pars, argTypes), os, resType),
        List("Content-Type" -> "application/json"));
    }
  }

  def typeConvert(pars: Map[String, String],
    argTypes: String = defaultArgTypes): Map[String, Any] = {
    pars.map(x => (x._1, typeConvert(x._1, x._2, argTypes))).filter(
      (x) => x._2 != None)
  }

  def typeConvert(name: String, value: String, argTypes: String) = {
    argTypes match {
      case ArgTypes.strong => convert(name, value)
      case ArgTypes.string => value
      case s => throw new IllegalArgumentException(s)
    }
  }

  private def convert(name: String, value: String, strong: Boolean = true) = {
    val DateP = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
    val DateTimeP =
      """(\d\d\d\d)\-(\d\d)\-(\d\d)[T ]?(\d\d)\:(\d\d)\:(\d\d)""".r
    val DecimalP = """(-)?(\d+)(\.\d*)?""".r
    val StringP = """\'(.*)""".r
    value match {
      case "" => None
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
      case s => throw new IllegalArgumentException(
        "For argtypes=strong, strings must be" +
          " prefixed with ' (for " + name + "=" + value + ")")
    }
  }

  def json(databaseString: String, dbSchemaString: String, expr: String, pars: Map[String, Any],
    rType: Jsonizer.ResultType = defaultResultType): String = {
    val writer = new CharArrayWriter
    json(databaseString, dbSchemaString, expr, pars, writer, rType)
    writer.toString
  }

  def json(databaseString: String, dbSchemaString: String, expr: String, pars: Map[String, Any],
    os: OutputStream, rType: Jsonizer.ResultType) {
    val writer = new OutputStreamWriter(os, "UTF-8")
    json(databaseString, dbSchemaString, expr, pars, writer, rType)
    writer.flush
  }

  def json(databaseString: String, dbSchemaString: String, expr: String, pars: Map[String, Any],
    writer: Writer, rType: Jsonizer.ResultType) {

    val (jndiExpr, schema) = if (databaseString == "") 
    	("java:/comp/env/jdbc/uniso/query", "public")
    else 
    	("java:/comp/env/" + databaseString, if (dbSchemaString == "") "public" else dbSchemaString)

    val ctx = new javax.naming.InitialContext()
    val dataSource = {
      ctx.lookup(jndiExpr) match {
        case ds: javax.sql.DataSource => ds
        case x => error("not data source in jndi context: " + x)
      }
    }
    val conn: Connection = dataSource.getConnection

    val uniqueMetaDataRecognizationString = jndiExpr + schema
    var md: MetaData = null
    if (mapForMetaData.contains(uniqueMetaDataRecognizationString))
      md = mapForMetaData(uniqueMetaDataRecognizationString)
    else {
      md = metadata.JDBCMetaData("", schema)
      mapForMetaData += ((uniqueMetaDataRecognizationString, md))
    }

    try {
      Env update md
      Env update conn
      Jsonizer.jsonize(Query(expr, pars), writer, rType)
    } finally {
      conn.close()
    } //finally
  }

  def bindVariables(expr: String): List[String] = { //
    QueryParser.bindVariables(expr)
  }

}