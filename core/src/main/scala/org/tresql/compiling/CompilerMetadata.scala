package org.tresql.compiling

import java.sql._

import org.tresql.{Metadata, Resources}
import org.tresql.metadata.JDBCMetadata

trait CompilerMetadata {
  def metadata: Metadata
  def macros: Any
}

/** Implementation must have empty constructor so can be instantiated with {{{Class.newInstance}}} */
trait CompilerMetadataFactory {
  def create(conf: Map[String, String], logger: Resources): CompilerMetadata
}

private[tresql] object MetadataCache {
  def create(
    conf: Map[String, String],
    factory: CompilerMetadataFactory,
    logger: Resources
  ): CompilerMetadata = {
    if (md == null) md = factory.create(conf, logger) else logger.log(s"Returning cached metadata")
    md
  }
  private[this] var md: CompilerMetadata = null
}

class CompilerJDBCMetadataFactory extends CompilerMetadataFactory {
  override def create(conf: Map[String, String], logger: Resources) = {
    val driverClassName = conf.getOrElse("driverClass", null)
    val url = conf.getOrElse("url", null)
    val user = conf.getOrElse("user", null)
    val password = conf.getOrElse("password", null)
    val dbCreateScript = conf.getOrElse("dbCreateScript", null)
    val functions = conf.getOrElse("functionSignatures", null)
    val macrosClass = conf.get("macros")

    logger.log(s"Creating database metadata from: $url")

    Class.forName(driverClassName)
    val connection =
      if (user == null) DriverManager.getConnection(url)
      else DriverManager.getConnection(url, user, password)
    logger.log(s"Compiling using jdbc connection: $connection")
    if (dbCreateScript != null) {
      logger.log(s"Creating database for compiler from script $dbCreateScript...")
      new scala.io.BufferedSource(
        Option(getClass
          .getResourceAsStream(dbCreateScript))
          .getOrElse(new java.io.FileInputStream(dbCreateScript)))
        .mkString
        .split("//")
        .foreach { sql =>
          val st = connection.createStatement
          st.execute(sql)
          st.close
        }
      logger.log("Success")
    }
    new CompilerMetadata {
      override def metadata: Metadata =
        if (functions == null) JDBCMetadata(connection) else {
          val f = Class.forName(functions)
          new JDBCMetadata with CompilerFunctionMetadata {
            override def conn = connection
            override def compilerFunctionSignatures = f
          }
        }

      override def macros: Any =
        macrosClass.map(cn => Class.forName(cn).getDeclaredConstructor().newInstance()).getOrElse(null)
    }
  }
}
