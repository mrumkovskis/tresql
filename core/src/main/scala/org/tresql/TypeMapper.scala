package org.tresql
package metadata

import java.sql.Types
import scala.reflect.{Manifest, ManifestFactory}
import scala.collection.immutable.Map

trait TypeMapper {
  private val typeToVendorType:  Map[String, Map[String, String]] = Map(
    "Array[Byte]"             -> Map("postgresql" -> "bytea", "sql" -> "blob"),
    "base64Binary"            -> Map("postgresql" -> "bytea", "sql" -> "blob"),
    "boolean"                 -> Map("oracle" -> "char", "postgresql" -> "bool", "sql" -> "boolean"),
    "bytes"                   -> Map("postgresql" -> "bytea", "sql" -> "blob"),
    "date"                    -> Map("sql" -> "date"),
    "dateTime"                -> Map("sql" -> "timestamp"),
    "decimal"                 -> Map("sql" -> "numeric"),
    "double"                  -> Map("hsqldb" -> "float", "sql" -> "double precision"),
    "float"                   -> Map("hsqldb" -> "real", "postgresql" -> "real", "sql" -> "float"),
    "int"                     -> Map("oracle" -> "numeric(9)", "sql" -> "integer"),
    "integer"                 -> Map("sql" -> "numeric"),
    "long"                    -> Map("oracle" -> "numeric(18)", "sql" -> "bigint"),
    "short"                   -> Map("sql" -> "smallint"),
    "string"                  -> Map("sql" -> "clob", "oracle" -> "varchar2", "postgresql" -> "text", "hsqldb" -> "varchar"),
    "text"                    -> Map("sql" -> "clob", "oracle" -> "varchar2", "postgresql" -> "text", "hsqldb" -> "varchar"),
    "time"                    -> Map("sql" -> "time"),
    "Boolean"                 -> Map("oracle" -> "char", "postgresql" -> "bool", "sql" -> "boolean"),
    "java.lang.Boolean"       -> Map("oracle" -> "char", "postgresql" -> "bool", "sql" -> "boolean"),
    "Double"                  -> Map("hsqldb" -> "float", "sql" -> "double precision"),
    "java.lang.Double"        -> Map("hsqldb" -> "float", "sql" -> "double precision"),
    "Float"                   -> Map("hsqldb" -> "real", "postgresql" -> "real", "sql" -> "float"),
    "java.lang.Float"         -> Map("hsqldb" -> "real", "postgresql" -> "real", "sql" -> "float"),
    "Int"                     -> Map("oracle" -> "numeric(9)", "sql" -> "integer"),
    "java.lang.Integer"       -> Map("oracle" -> "numeric(9)", "sql" -> "integer"),
    "Long"                    -> Map("oracle" -> "numeric(18)", "sql" -> "bigint"),
    "java.lang.Long"          -> Map("oracle" -> "numeric(18)", "sql" -> "bigint"),
    "Short"                   -> Map("sql" -> "smallint"),
    "java.lang.Short"         -> Map("sql" -> "smallint"),
    "java.sql.Date"           -> Map("sql" -> "date"),
    "java.sql.Time"           -> Map("sql" -> "time"),
    "java.sql.Timestamp"      -> Map("sql" -> "timestamp"),
    "java.time.LocalDate"     -> Map("sql" -> "date"),
    "java.time.LocalDateTime" -> Map("sql" -> "timestamp"),
    "java.time.LocalTime"     -> Map("sql" -> "time"),
    "java.util.Date"          -> Map("sql" -> "date"),
    "scala.math.BigDecimal"   -> Map("sql" -> "numeric"),
    "scala.math.BigInt"       -> Map("sql" -> "numeric"),
    "String"                  -> Map("sql" -> "clob", "oracle" -> "varchar2", "postgresql" -> "text", "hsqldb" -> "varchar"),
    "java.lang.String"        -> Map("sql" -> "clob", "oracle" -> "varchar2", "postgresql" -> "text", "hsqldb" -> "varchar"),
  )
  def to_sql_type(vendor: String, typeName: String): String =
    typeToVendorType.get(typeName).flatMap(vt => vt.get(vendor).orElse(vt.get("sql"))) getOrElse typeName

  def to_scala_type(typeName: String): String = xsd_scala_type_map(typeName).toString()
  def sql_scala_type_map(jdbcTypeCode: Int): Manifest[_] = xsd_scala_type_map(from_jdbc_type(jdbcTypeCode))

  def xsd_scala_type_map(xsdType: String): Manifest[_] = xsdType match {
    case "integer" => ManifestFactory.classType(classOf[java.lang.Long])
    case "long" => ManifestFactory.classType(classOf[java.lang.Long])
    case "short" | "int" => ManifestFactory.classType(classOf[java.lang.Integer])
    case "float" | "double" | "decimal" => ManifestFactory.classType(classOf[BigDecimal])
    case "date" => ManifestFactory.classType(classOf[java.sql.Date])
    case "dateTime" | "timestamp" => ManifestFactory.classType(classOf[java.sql.Timestamp])
    case "time" => ManifestFactory.classType(classOf[java.sql.Time])
    case "string" => ManifestFactory.classType(classOf[String])
    case "text"   => ManifestFactory.classType(classOf[String])
    case "boolean" => ManifestFactory.classType(classOf[java.lang.Boolean])
    case "base64Binary" | "bytes" => ManifestFactory.classType(classOf[Array[Byte]])
    case "anyType" | "any" => ManifestFactory.Any
    case "unit" => Manifest.Unit
    case _ => ManifestFactory.Any
  }

  def from_jdbc_type(jdbcTypeCode: Int): String = jdbcTypeCode match {
    case Types.ARRAY                    => "bytes"
    case Types.BIGINT                   => "long"
    case Types.BINARY                   => "bytes"
    case Types.BIT                      => "boolean"
    case Types.BLOB                     => "bytes"
    case Types.BOOLEAN                  => "boolean"
    case Types.CHAR                     => "string"
    case Types.CLOB                     => "string"
    case Types.DATALINK                 => "string"
    case Types.DATE                     => "date"
    case Types.DECIMAL                  => "decimal"
    case Types.DISTINCT                 => "string"
    case Types.DOUBLE                   => "double"
    case Types.FLOAT                    => "double"
    case Types.INTEGER                  => "int"
    case Types.JAVA_OBJECT              => "bytes"
    case Types.LONGNVARCHAR             => "string"
    case Types.LONGVARBINARY            => "bytes"
    case Types.LONGVARCHAR              => "string"
    case Types.NCHAR                    => "string"
    case Types.NCLOB                    => "string"
    case Types.NULL                     => "string"
    case Types.NUMERIC                  => "decimal"
    case Types.NVARCHAR                 => "string"
    case Types.OTHER                    => "any"
    case Types.REAL                     => "float"
    case Types.REF                      => "string"
    case Types.REF_CURSOR               => "string"
    case Types.ROWID                    => "string"
    case Types.SMALLINT                 => "short"
    case Types.SQLXML                   => "string"
    case Types.STRUCT                   => "bytes"
    case Types.TIME                     => "time"
    case Types.TIME_WITH_TIMEZONE       => "time"
    case Types.TIMESTAMP                => "dateTime"
    case Types.TIMESTAMP_WITH_TIMEZONE  => "dateTime"
    case Types.TINYINT                  => "short"
    case Types.VARBINARY                => "bytes"
    case Types.VARCHAR                  => "string"
    case x => sys.error("Unexpected jdbc type code: " + x)
  }
}

object TypeMapper {
  def scalaToJdbc(scalaTypeName: String): Int = scalaTypeName match {
    case "Array[Byte]"              => Types.VARBINARY
    case "Boolean"                  => Types.BOOLEAN
    case "Double"                   => Types.DOUBLE
    case "Int"                      => Types.INTEGER
    case "java.io.InputStream"      => Types.LONGVARBINARY
    case "java.io.Reader"           => Types.LONGVARCHAR
    case "java.lang.Boolean"        => Types.BOOLEAN
    case "java.lang.Double"         => Types.DOUBLE
    case "java.lang.Integer"        => Types.INTEGER
    case "java.lang.Long"           => Types.BIGINT
    case "java.lang.String"         => Types.VARCHAR
    case "java.math.BigDecimal"     => Types.DECIMAL
    case "java.math.BigInteger"     => Types.NUMERIC
    case "java.sql.Blob"            => Types.BLOB
    case "java.sql.Clob"            => Types.CLOB
    case "java.sql.Date"            => Types.DATE
    case "java.sql.Time"            => Types.TIME
    case "java.sql.Timestamp"       => Types.TIMESTAMP
    case "java.time.LocalDate"      => Types.DATE
    case "java.time.LocalDateTime"  => Types.TIMESTAMP
    case "java.time.LocalTime"      => Types.TIME
    case "java.util.Date"           => Types.TIMESTAMP
    case "Long"                     => Types.BIGINT
    case "scala.math.BigDecimal"    => Types.DECIMAL
    case "scala.math.BigInt"        => Types.NUMERIC
    case _                          => Types.OTHER
  }
}
