package org.tresql.compiling

import java.sql._
import org.tresql.{Env, Metadata}
import org.tresql.metadata.JDBCMetadata

trait CompilerMetadata {
  def metadata: Metadata
  def macros: Option[Any]
}

/** Implementation must have empty constructor so can be instantiated with {{{Class.newInstance}}} */
trait CompilerMetadataFactory {
  def create(conf: Map[String, String]): CompilerMetadata
}

private[tresql] object MetadataCache {
  def create(
    conf: Map[String, String],
    factory: CompilerMetadataFactory
  ): CompilerMetadata = {
    if (md == null) md = factory.create(conf)
    md
  }
  private[this] var md: CompilerMetadata = null
}

class CompilerJDBCMetadataFactory extends CompilerMetadataFactory {
  override def create(conf: Map[String, String]) = {
    val driverClassName = conf.getOrElse("driverClass", null)
    val url = conf.getOrElse("url", null)
    val user = conf.getOrElse("user", null)
    val password = conf.getOrElse("password", null)
    val dbCreateScript = conf.getOrElse("dbCreateScript", null)
    val functions = conf.getOrElse("functionSignatures", null)
    val macrosClass = conf.get("macros")

    Env.logger = (msg, _, _) => println(msg)
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
    new CompilerMetadata {
      override def metadata: Metadata =
        if (functions == null) JDBCMetadata() else {
          val f = Class.forName(functions)
          new JDBCMetadata with CompilerFunctionMetadata {
            override def compilerFunctionSignatures = f
          }
        }

      override def macros: Option[Any] =
        macrosClass.map(cn => Class.forName(cn).getDeclaredConstructor().newInstance())
    }
  }
}
