package org.tresql.compiling

trait DBAggregateFunctions {
  //aggregate functions
  def count(col: Any): java.lang.Number
  def max(col: Any): Any
  def min(col: Any): Any
  def sum(col: java.lang.Number): java.lang.Number
  def avg(col: java.lang.Number): java.lang.Number
}

trait Functions extends DBAggregateFunctions {
  def coalesce[T](pars: T*): T
  def upper(string: String): String
  def lower(string: String): String
  def insert (str1: String, offset: Int, length: Int, str2: String): String
  def to_date(date: String, format: String): java.sql.Date
}
