package org.tresql.java_api

import java.sql.Connection

import org.tresql._
import org.tresql.{ Env => env }
import org.tresql.metadata.JDBCMetadata

trait IdExprFunc { def getIdExpr(table: String): String }
trait LogMessage { def get: String }
trait LogParams { def get: Map[String, Any] }
trait Logger { def log(msg: LogMessage, params: LogParams, topic: LogTopic): Unit }
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
  def getIdExprFunc: IdExprFunc = new IdExprFunc {
    override def getIdExpr(table: String) = env.idExpr(table)
  }
  def setIdExprFunc(f: IdExprFunc) { env.idExpr = f.getIdExpr }
  //def isDefined(functionName: String): Boolean
  def getMetadata: Metadata = env.metadata
  def setMetadata(md: Metadata) { env.metadata = md }
  //def getNameMap: NameMap = env.nameMap
  //def setNameMap(m: NameMap) = { env.nameMap = m }
  //var sharedConn: Connection
  //def tableName(objectName: String): String
  def setLogger(logger: Logger) {
    env.logger = (msg, params, topic) => logger.log(new LogMessage {
      override def get = msg
    }, new LogParams { override def get = params }, topic)
  }
  def getLogger: Logger = new Logger {
    private[this] val logger = env.logger
    override def log(msg: LogMessage, params: LogParams, topic: LogTopic) = logger(msg.get, params.get, topic)
  }
  //def update(map: (Map[String, String], Map[String, Map[String, (String, String)]])): Unit
  //def valueExpr(objectName: String, propertyName: String): String
  case class State(
    cache: Option[Cache],
    conn: Connection,
    dialect: Dialect,
    idExpr: String => String,
    metadata: Metadata,
    logger: (=> String, => Map[String, Any], LogTopic) => Unit
  )
  def saveState = State(
    cache = env.cache,
    conn = env.conn,
    dialect = env.dialect,
    idExpr = env.idExpr,
    metadata = env.metadata,
    logger = env.logger
  )
  def restoreState(state: State) = {
    import state._
    env.cache = cache.orNull
    env.conn = conn
    env.dialect = dialect
    env.idExpr = idExpr
    env.metadata = metadata
    env.logger = logger
  }
}
