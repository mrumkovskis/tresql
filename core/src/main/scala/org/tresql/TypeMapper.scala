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
  def sql_scala_type_map(sqlType: Int): Manifest[_] = xsd_scala_type_map(sql_xsd_type_map(sqlType))
  def xsd_scala_type_map(xsdType: String): Manifest[_] = xsdType match {
    case "integer" => ManifestFactory.classType(classOf[java.lang.Long])
    case "long" => ManifestFactory.classType(classOf[java.lang.Long])
    case "short" | "int" => ManifestFactory.classType(classOf[java.lang.Integer])
    case "float" | "double" | "decimal" => ManifestFactory.classType(classOf[BigDecimal])
    case "date" => ManifestFactory.classType(classOf[java.sql.Date])
    case "dateTime" | "timestamp" => ManifestFactory.classType(classOf[java.sql.Timestamp])
    case "time" => ManifestFactory.classType(classOf[java.sql.Time])
    case "string" => ManifestFactory.classType(classOf[String])
    case "boolean" => ManifestFactory.classType(classOf[java.lang.Boolean])
    case "base64Binary" | "bytes" => ManifestFactory.classType(classOf[Array[Byte]])
    case "anyType" | "any" => ManifestFactory.Any
    case "unit" => Manifest.Unit
    case _ => ManifestFactory.Any
  }
  def scala_xsd_type_map(scalaType: String): String = scalaType match {
    case "java.lang.Long" => "long"
    case "java.lang.Integer" => "int"
    case "scala.math.BigDecimal" => "decimal"
    case "java.sql.Date" => "date"
    case "java.sql.Timestamp" => "dateTime"
    case "java.sql.Time" => "time"
    case "java.lang.String" => "string"
    case "java.lang.Boolean" => "boolean"
    case "Array[Byte]" => "base64Binary"
    case "Any" => "anyType"
    case _ => "anyType"
  }
  def sql_xsd_type_map(sqlType: Int): String = sqlType match {
    case Types.ARRAY => "base64Binary"
    case Types.BIGINT => "long"
    case Types.BINARY => "base64Binary"
    case Types.BIT => "boolean"
    case Types.BLOB => "base64Binary"
    case Types.BOOLEAN => "boolean"
    case Types.CHAR => "string"
    case Types.CLOB => "string"
    case Types.DATALINK => "string"
    case Types.DATE => "date"
    case Types.DECIMAL | Types.NUMERIC => "decimal"
    case Types.DISTINCT => "string"
    case Types.DOUBLE => "double"
    case Types.FLOAT => "double"
    case Types.INTEGER => "int"
    case Types.JAVA_OBJECT => "base64Binary"
    case Types.LONGNVARCHAR => "string"
    case Types.LONGVARBINARY => "base64Binary"
    case Types.LONGVARCHAR => "string"
    case Types.NCHAR => "string"
    case Types.NCLOB => "string"
    case Types.NULL => "string"
    case Types.NVARCHAR => "string"
    case Types.OTHER => "base64Binary"
    case Types.REAL => "float"
    case Types.REF => "string"
    case Types.ROWID => "string"
    case Types.SMALLINT => "short"
    case Types.SQLXML => "string"
    case Types.STRUCT => "base64Binary"
    case Types.TIME => "time"
    case Types.TIMESTAMP => "dateTime"
    case Types.TINYINT => "short"
    case Types.VARBINARY => "base64Binary"
    case Types.VARCHAR => "string"
    case x => sys.error("Unexpected jdbc type code: " + x)
  }
}
