package org.tresql.test

trait TestFunctionSignatures extends org.tresql.compiling.TresqlFunctionSignatures {
  def concat(string: String*): String
  def macro_interpolator_test1(exp1: Any, exp2: Any): Any
  def macro_interpolator_test2(exp1: Any, exp2: Any): Any
  def macro_interpolator_test3(exp1: Any, exp2: Any): Any
  def in_twice(expr: Any, in: Any): Boolean
  //test macros function
  def null_macros: Null
  def inc_val_5(int: java.lang.Integer): java.lang.Integer
  def dummy(): Any
  def dummy_table(): Any
  def pi(): Double
  def truncate(param: Any): Int
  //postgres dialect
  def to_char(a: BigDecimal, p: Any): String
  def trunc(a: BigDecimal, i: java.lang.Integer): BigDecimal
  def trunc(a: BigDecimal): BigDecimal
  def round(a: BigDecimal, i: java.lang.Integer): BigDecimal
  def date_part(field: Any, source: Any): Any
  def isfinite(field: Any): Any
  def unnest(arr: Any): Any
  def sequence_array(el: Any*): Any
  def generate_series(args: Any*): Any
  def substring(str: String, from: Int, length: Int): String
  def position(pos: Any): Int
}
