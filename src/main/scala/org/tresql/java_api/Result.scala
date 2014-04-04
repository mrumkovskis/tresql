package org.tresql.java_api

trait Result extends Row with java.lang.Iterable[Row] with java.util.Iterator[Row] {
  def jdbcResult: java.sql.ResultSet
  def close: Unit
  /* TODO
  def toList: java.util.List[Row]
  */
  def toListOfMaps: java.util.List[java.util.Map[String, Object]]
  /**
   * iterates through this result rows as well as these of descendant result
   * ensures execution of dml (update, insert, delete) expressions in colums otherwise
   * has no side effect.
   */
  def execute: Unit
}

trait Row {
  def get(idx: Int): Object
  def get(name: String): Object
  def int_(idx: Int): Int
  def int_(name: String): Int
  def i(idx: Int): Int
  def i(name: String): Int
  def long_(idx: Int): Long
  def long_(name: String): Long
  def l(idx: Int): Long
  def l(name: String): Long
  def double_(idx: Int): Double
  def double_(name: String): Double
  def dbl(idx: Int): Double
  def dbl(name: String): Double
  def string(idx: Int): String
  def string(name: String): String
  def s(idx: Int): String
  def s(name: String): String
  def date(idx: Int): java.sql.Date
  def date(name: String): java.sql.Date
  def d(idx: Int): java.sql.Date
  def d(name: String): java.sql.Date
  def timestamp(idx: Int): java.sql.Timestamp
  def timestamp(name: String): java.sql.Timestamp
  def t(idx: Int): java.sql.Timestamp
  def t(name: String): java.sql.Timestamp
  def boolean_(idx: Int): Boolean
  def boolean_(name: String): Boolean
  def bl(idx: Int): Boolean
  def bl(name: String): Boolean
  def bytes(idx: Int): Array[Byte]
  def bytes(name: String): Array[Byte]
  def stream(idx: Int): java.io.InputStream
  def stream(name: String): java.io.InputStream
  def result(idx: Int): Result
  def result(name: String): Result
  def jInt(idx: Int): java.lang.Integer
  def jInt(name: String): java.lang.Integer
  def ji(idx: Int): java.lang.Integer
  def ji(name: String): java.lang.Integer
  def jLong(idx: Int): java.lang.Long
  def jLong(name: String): java.lang.Long
  def jl(idx: Int): java.lang.Long
  def jl(name: String): java.lang.Long
  def jBoolean(idx: Int): java.lang.Boolean
  def jBoolean(name: String): java.lang.Boolean
  def jbl(idx: Int): java.lang.Boolean
  def jbl(name: String): java.lang.Boolean
  def jDouble(idx: Int): java.lang.Double
  def jDouble(name: String): java.lang.Double
  def jdbl(idx: Int): java.lang.Double
  def jdbl(name: String): java.lang.Double
  def jBigDecimal(idx: Int): java.math.BigDecimal
  def jBigDecimal(name: String): java.math.BigDecimal
  def jbd(idx: Int): java.math.BigDecimal
  def jbd(name: String): java.math.BigDecimal
  def columnCount: Int
  def column(idx: Int): Column
  def columns: java.util.List[Column]
  /* TODO
  def values: java.util.List[Object]
  */
  def rowToMap: java.util.Map[String, Object]
}
