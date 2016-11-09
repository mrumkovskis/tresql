package org.tresql.compiling

trait DBAggregateFunctions {
  //aggregate functions
  def count(col: Any): java.lang.Long
  def max[T](col: T): T
  def min[T](col: T): T
  def sum[T](col: T): T
  def avg[T](col: T): T
}

trait Functions extends DBAggregateFunctions {
  def coalesce[T](pars: T*): T
  def upper(string: String): String
  def lower(string: String): String
  def insert (str1: String, offset: Int, length: Int, str2: String): String
  def to_date(date: String, format: String): java.sql.Date
  def trim(string: String): String
  //macros
  def if_defined[T](variable: Any, exp: T): T
  //dialect
  def `case`[T](when: Any, then: T, rest: Any*): T
}
