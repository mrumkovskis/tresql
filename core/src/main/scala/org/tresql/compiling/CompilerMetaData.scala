package org.tresql.compiling

import java.sql._
import org.tresql.{Env, MetaData}
import org.tresql.metadata.JDBCMetaData

/** Implementation must have empty constructor so can be instantiated with {{{Class.newInstance}}} */
trait CompilerMetaData {
  def create(conf: Map[String, String]): MetaData
}

private object Metadata {
  var md: MetaData = null
}

class CompilerJDBCMetaData extends CompilerMetaData {
  override def create(conf: Map[String, String]) = {
    val driverClassName = conf.getOrElse("driverClass", null)
    val url = conf.getOrElse("url", null)
    val user = conf.getOrElse("user", null)
    val password = conf.getOrElse("password", null)
    val dbCreateScript = conf.getOrElse("dbCreateScript", null)
    val functions = conf.getOrElse("functions", null)

    if (Metadata.md != null) Metadata.md else {
      Env.logger = ((msg, level) => println (msg))
      Class.forName(driverClassName)
      val conn =
        if (user == null) DriverManager.getConnection(url)
        else DriverManager.getConnection(url, user, password)
      Env.sharedConn = conn
      if (dbCreateScript != null) {
        Env.log(s"Creating database for compiler from script $dbCreateScript...")
        new scala.io.BufferedSource(
          Option(getClass
            .getResourceAsStream(dbCreateScript))
            .getOrElse(new java.io.FileInputStream(dbCreateScript)))
          .mkString
          .split("//")
          .foreach { sql =>
            val st = conn.createStatement
            st.execute(sql)
            st.close
          }
        Env.log("Success")
      }
      Metadata.md = if (functions == null) JDBCMetaData() else {
        val f = Class.forName(functions)
        new JDBCMetaData with CompilerFunctions {
          override def compilerFunctions = f
        }
      }
      Metadata.md
    }
  }
}
