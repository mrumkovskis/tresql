package org.tresql.java_api

import java.sql.Connection

import org.tresql._
import org.tresql.{ Env => env }
import org.tresql.metadata.JDBCMetadata

trait IdExprFunc { def getIdExpr(table: String): String }
trait LogMessage { def get: String }
trait Logger { def log(msg: LogMessage, level: Int): Unit }
object Dialects {
  def ANSISQL = dialects.ANSISQLDialect
  def HSQL = dialects.HSQLDialect
  def Oracle = dialects.OracleDialect
}
object Metadata {
  def JDBC(defaultSchema: String) = JDBCMetadata(defaultSchema)
}
object Env {
  import CoreTypes._

  //def apply(params: Map[String, Any], reusableExpr: Boolean): Env
  def getCache: Cache = env.cache.orNull
  def setCache(c: Cache) { env.cache = c }
  //def colName(objectName: String, propertyName: String): String
  def getConnection: Connection = env.conn
  def setConnection(c: Connection) { env.conn = c }
  def getDialect: Dialect = env.dialect
  def setDialect(d: Dialect) { env.dialect = d }
  def getFunctions: Object = env.functions.map(_.asInstanceOf[AnyRef]).orNull
  def setFunctions(funcs: Object): Unit = { env.functions = funcs }
  def getIdExprFunc: IdExprFunc = new IdExprFunc {
    override def getIdExpr(table: String) = env.idExpr(table)
  }
  def setIdExprFunc(f: IdExprFunc) { env.idExpr = f.getIdExpr }
  //def isDefined(functionName: String): Boolean
  //def log(msg: => String, level: Int = 0): Unit
  def getMetadata: Metadata = env.metaData
  def setMetadata(md: Metadata) { env.metaData = md }
  //def getNameMap: NameMap = env.nameMap
  //def setNameMap(m: NameMap) = { env.nameMap = m }
  //var sharedConn: Connection
  //def tableName(objectName: String): String
  def setLogger(logger: Logger) {
    env.logger = (msg, level) => logger.log(new LogMessage {
      override def get = msg
    }, level)
  }
  def getLogger: Logger = new Logger {
    private[this] val logger = env.logger
    override def log(msg: LogMessage, level: Int) = logger(msg.get, level)
  }
  //def update(map: (Map[String, String], Map[String, Map[String, (String, String)]])): Unit
  //def valueExpr(objectName: String, propertyName: String): String
  case class State(
    cache: Option[Cache],
    conn: Connection,
    dialect: Dialect,
    functions: Object,
    idExpr: String => String,
    metadata: Metadata,
    logger: (=> String, Int) => Unit
  )
  def saveState = State(
    cache = env.cache,
    conn = env.conn,
    dialect = env.dialect,
    functions = env.functions,
    idExpr = env.idExpr,
    metadata = env.metaData,
    logger = env.logger
  )
  def restoreState(state: State) = {
    import state._
    env.cache = cache.orNull
    env.conn = conn
    env.dialect = dialect
    env.functions = functions
    env.idExpr = idExpr
    env.metaData = metadata
    env.logger = logger
  }
}
