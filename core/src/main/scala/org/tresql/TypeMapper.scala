package org.tresql
package metadata

import java.sql.Types
import scala.reflect.{Manifest, ManifestFactory}

trait TypeMapper {
  def sql_scala_type_map(sqlType: Int): Manifest[_] = xsd_scala_type_map(sql_xsd_type_map(sqlType))
  def xsd_scala_type_map(xsdType: String): Manifest[_] = xsdType match {
    case "integer" => ManifestFactory.classType(classOf[java.lang.Long])
    case "long" => ManifestFactory.classType(classOf[java.lang.Long])
    case "short" | "int" => ManifestFactory.classType(classOf[java.lang.Integer])
    case "float" | "double" | "decimal" => ManifestFactory.classType(classOf[BigDecimal])
    case "date" => ManifestFactory.classType(classOf[java.sql.Date])
    case "time" | "dateTime" | "timestamp" => ManifestFactory.classType(classOf[java.sql.Timestamp])
    case "string" => ManifestFactory.classType(classOf[String])
    case "boolean" => ManifestFactory.classType(classOf[java.lang.Boolean])
    case "base64Binary" => ManifestFactory.classType(classOf[Array[Byte]])
    case "anyType" => ManifestFactory.Any
    case _ => ManifestFactory.Any
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
