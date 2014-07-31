package org.tresql.java_api

import java.sql.Connection

import org.tresql._
import org.tresql.{ Env => env }
import org.tresql.metadata.JDBCMetaData

trait IdExprFunc { def getIdExpr(table: String): String }
trait LogMessage { def get: String }
trait Logger { def log(msg: LogMessage, level: Int): Unit }
object Dialects {
  def ANSISQL = dialects.ANSISQLDialect
  def HSQL = dialects.HSQLDialect
  def Oracle = dialects.OracleDialect
  def InsensitiveCmp(accents: String, normalized: String) =
    dialects.InsensitiveCmp(accents, normalized)
}
object Metadata {
  def JDBC(defaultSchema: String) = JDBCMetaData(defaultSchema)
}
object Env {
  //def apply(params: Map[String, Any], reusableExpr: Boolean): Env
  def getCache: Cache = env.cache.orNull
  def setCache(c: Cache) { env.cache = c }
  //def colName(objectName: String, propertyName: String): String
  def getConnection: Connection = env.conn
  def setConnection(c: Connection) { env.conn = c }
  def getDialect: PartialFunction[Expr, String] = env.dialect
  def setDialect(d: PartialFunction[Expr, String]) { env.dialect = d }
  def getFunctions: Object = env.functions.map(_.asInstanceOf[AnyRef]).orNull
  def setFunctions(funcs: Object): Unit = { env.functions = funcs }
  def getIdExprFunc: IdExprFunc = new IdExprFunc {
    override def getIdExpr(table: String) = env.idExpr(table)
  }
  def setIdExprFunc(f: IdExprFunc) { env.idExpr = f.getIdExpr }
  //def isDefined(functionName: String): Boolean
  //def log(msg: => String, level: Int = 0): Unit 
  def getMetadata: MetaData = env.metaData
  def setMetadata(md: MetaData) { env.metaData = md }
  //def getNameMap: NameMap = env.nameMap
  //def setNameMap(m: NameMap) = { env.nameMap = m }
  //var sharedConn: Connection 
  //def tableName(objectName: String): String 
  def setLogger(logger: Logger) {
    env.update((msg, level) => logger.log(new LogMessage {
      override def get = msg
    }, level))
  }
  //def update(map: (Map[String, String], Map[String, Map[String, (String, String)]])): Unit
  //def valueExpr(objectName: String, propertyName: String): String
}
