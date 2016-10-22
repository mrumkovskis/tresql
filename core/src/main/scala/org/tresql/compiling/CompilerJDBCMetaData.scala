package org.tresql.compiling

import java.sql._
import org.tresql.{Env, MetaData}
import org.tresql.metadata.JDBCMetaData

/** Implementation must have empty constructor so can be instantiated with {{{Class.newInstance}}} */
trait CompilerMetaData {
  def create(
    driverClassName: String,
    url: String,
    user: String,
    password: String,
    dbCreateScript: String
  ): MetaData
}

private object Metadata {
  var md: MetaData = null
}

class CompilerJDBCMetaData extends CompilerMetaData {
  def create(
    driverClassName: String,
    url: String,
    user: String,
    password: String,
    dbCreateScript: String
  ) = {
    if (Metadata.md != null) Metadata.md else {
      Env.logger = ((msg, level) => println (msg))
      Class.forName(driverClassName)
      val conn =
        if (user == null) DriverManager.getConnection(url)
        else DriverManager.getConnection(url, user, password)
      Env.sharedConn = conn
      if (dbCreateScript != null) {
        Env.log("Creating database for compiler...")
        new scala.io.BufferedSource(getClass.getResourceAsStream(dbCreateScript))
          .mkString
          .split("//")
          .foreach { sql =>
            val st = conn.createStatement
            st.execute(sql)
            st.close
          }
        Env.log("Success")
      }
      Metadata.md = JDBCMetaData()
      Metadata.md
    }
  }
}
