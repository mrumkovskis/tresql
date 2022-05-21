package org.tresql.compiling

import java.sql._

import org.tresql.Metadata
import org.tresql.metadata.JDBCMetadata

trait CompilerMetadata {
  def metadata: Metadata
  def extraMetadata: Map[String, Metadata]
  def macros: Any
}

/** Implementation must have empty constructor so can be instantiated with {{{Class.newInstance}}} */
trait CompilerMetadataFactory {
  def create(conf: Map[String, String]): CompilerMetadata
}

private[tresql] object MetadataCache {
  def create(
    conf: Map[String, String],
    factory: CompilerMetadataFactory,
    verbose: Boolean
): CompilerMetadata = {
    if (md == null) md = factory match {
      case f: CompilerJDBCMetadataFactory => f.create(conf, verbose)
      case f => f.create(conf)
    } else {
      if (verbose) println(s"Returning cached metadata")
    }
    md
  }
  private[this] var md: CompilerMetadata = null
}

class CompilerJDBCMetadataFactory extends CompilerMetadataFactory {

  case class Conf(driverClassName: String,
                  url: String,
                  user: String,
                  password: String,
                  dbCreateScript: String)

  def create(conf: Map[String, String]): CompilerMetadata = create(conf, false)
  def create(conf: Map[String, String], verbose: Boolean): CompilerMetadata = {
    val macrosClazz = conf.get("macros_class").map(Class.forName)
    val jdbc_metadata = {
      def createMetadata(mdConf: Conf) = {
        import mdConf._
        if (verbose) println(s"Creating database metadata from: $url")

        Class.forName(driverClassName)
        val connection =
          if (user == null) DriverManager.getConnection(url)
          else DriverManager.getConnection(url, user, password)
        if (verbose) println(s"Compiling using jdbc connection: $connection")
        if (dbCreateScript != null) {
          if (verbose) println(s"Creating database for compiler from script $dbCreateScript...")
          new scala.io.BufferedSource(
            Option(this.getClass
              .getResourceAsStream(dbCreateScript))
              .getOrElse(new java.io.FileInputStream(dbCreateScript)))
            .mkString
            .split("//")
            .foreach { sql =>
              val st = connection.createStatement
              st.execute(sql)
              st.close
            }
          if (verbose) println("Success")
        }
        new JDBCMetadata {
          override def conn = connection
          override def macroClass: Class[_] = macrosClazz.orNull
        }
      }
      def createConf(c: Map[String, String]) = {
        Conf(
          c.getOrElse("jdbc_driver_class" , null),
          c.getOrElse("jdbc_url"          , null),
          c.getOrElse("jdbc_user"         , null),
          c.getOrElse("jdbc_password"     , null),
          c.getOrElse("db_create_script"  , null),
        )
      }
      def param(k: String) = {
        val i = k.indexOf(".")
        if (i == -1) k else k.substring(0, i)
      }
      def db(k: String) = {
        val i = k.indexOf(".")
        if (i == -1) "" else k.substring(i + 1)
      }
      conf
        //.groupMapReduce { case (k, _) => db(k) } { case (k, v) => Map(param(k) -> v) } (_ ++ _) // scala 2.13.x
        .groupBy { case (k, _) => db(k) }
        .mapValues(_.map { case (k, v) => Map(param(k) -> v) }.reduce(_ ++ _))
        .map { case (k, c) => (k, createMetadata(createConf(c))) }
        .toMap
    }

    new CompilerMetadata {
      override def metadata: Metadata = jdbc_metadata("")
      /** Currently no extra metadata are supported */
      override def extraMetadata: Map[String, Metadata] = jdbc_metadata.filterNot(_._1 == "")
      override def macros: Any = {
        macrosClazz.map(_.getDeclaredConstructor().newInstance()).orNull
      }
    }
  }
}
