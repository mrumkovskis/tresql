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
  def inc_val_5(int: java.lang.Integer): java.lang.Integer
  //macros
  def if_defined[T](variable: Any, exp: T): T
  def if_missing[T](variable: Any, exp: T): T
  def if_any_defined(exp: Any*): Any
  def if_all_defined(exp: Any*): Any
  def if_any_missing(exp: Any*): Any
  def if_all_missing(exp: Any*): Any
  def sql_concat(exprs: Any*): Any
  def sql(expr: Any): Any
  //test macros function
  def null_macros: Null
  //dialect
  def `case`[T](when: Any, `then`: T, rest: Any*): T
  def nextval(seq: String): Any
  //tresql scala functions
  import org.tresql.QueryCompiler._
  def mkString(sel: SelectDefBase): String
  def mkString(sel: SelectDefBase, colSep: String): String
  def mkString(sel: SelectDefBase, colSep: String, rowSep: String)
  def concat(string: String*): String
  //test tresql scala functions (must be registered here to be accessible during compile time)
  def echo(x: String): String
  def plus(a: java.lang.Long, b: java.lang.Long): java.lang.Long
  def average(a: BigDecimal, b: BigDecimal): BigDecimal
  def dept_desc(d: String, ec: String): String
  def nopars(): String
}
