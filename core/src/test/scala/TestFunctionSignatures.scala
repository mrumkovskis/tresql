package org.tresql.test

trait TestFunctionSignatures extends org.tresql.compiling.TresqlFunctionSignatures {
  import org.tresql.QueryCompiler._
  def mkString(sel: SelectDefBase): String
  def mkString(sel: SelectDefBase, colSep: String): String
  def mkString(sel: SelectDefBase, colSep: String, rowSep: String)
  def concat(string: String*): String
  def macro_interpolator_test1(exp1: Any, exp2: Any): Any
  def macro_interpolator_test2(exp1: Any, exp2: Any): Any
  def macro_interpolator_test3(exp1: Any, exp2: Any): Any
  def in_twice(expr: Any, in: Any): Boolean
  //test macros function
  def null_macros: Null
  //test tresql scala functions (must be registered here to be accessible during compile time)
  def echo(x: String): String
  def plus(a: java.lang.Long, b: java.lang.Long): java.lang.Long
  def average(a: BigDecimal, b: BigDecimal): BigDecimal
  def dept_desc(d: String, ec: String): String
  def nopars(): String
  def inc_val_5(int: java.lang.Integer): java.lang.Integer
  def dummy(): Any
  def dept_count(): java.lang.Integer
  def dept_desc_with_empc(d: String): String
  def vararg_with_resources(s: String*): String
  def pi(): Double
  def truncate(param: Any): Int
}
