package org.tresql.java_api

import java.sql.Connection

import org.tresql._
import org.tresql.metadata.JDBCMetadata
import org.tresql.{Env => SEnv, ThreadLocalResources => TLR}

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
  def JDBC(conn: Connection, defaultSchema: String) = JDBCMetadata(conn, defaultSchema)
}

trait ThreadLocalResources extends TLR {
  import CoreTypes._

  def getConnection: Connection = conn
  def setConnection(c: Connection) { conn = c }
  def getDialect: Dialect = dialect
  def setDialect(d: Dialect) { dialect = d }
  def getIdExprFunc: IdExprFunc = new IdExprFunc {
    override def getIdExpr(table: String) = idExpr(table)
  }
  def setIdExprFunc(f: IdExprFunc) { idExpr = f.getIdExpr }
  def getMetadata: Metadata = metadata
  def setMetadata(md: Metadata) { metadata = md }
}

object Env {
  def getCache: Cache = SEnv.cache.orNull
  def setCache(c: Cache) { SEnv.cache = c }
  def setLogger(logger: Logger) {
    SEnv.logger = (msg, params, topic) => logger.log(new LogMessage {
      override def get = msg
    }, new LogParams { override def get = params }, topic)
  }
  def getLogger: Logger = new Logger {
    private[this] val logger = SEnv.logger
    override def log(msg: LogMessage, params: LogParams, topic: LogTopic) =
      logger(msg.get, params.get, topic)
  }
}
