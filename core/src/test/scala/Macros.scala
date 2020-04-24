package org.tresql.test

import org.tresql.{Expr, QueryBuilder, macro_, parsing}

class Macros extends org.tresql.Macros {
  import macro_._
  /**
   * Dumb regexp to find bind variables (tresql syntax) in sql string.
   * Expects whitespace, colon, identifier, optional question mark.
   * Whitespace before colon is a workaround to ignore postgresql typecasts.
   */
  private val varRegex = "\\s:[_a-zA-Z]\\w*\\??"r
  override def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr): b.SQLExpr = {
    val value = String.valueOf(const.value)
    val vars = varRegex.findAllIn(value).toList
      .map(_ substring 2)
      .map(v => b.VarExpr(v.replace("?", ""), Nil, v endsWith "?"))
    val sqlSnippet = varRegex.replaceAllIn(value, " ?")
    if (vars.exists(v => v.opt && !(b.env contains v.name)))
      b.SQLExpr("null", Nil)
    else b.SQLExpr(sqlSnippet, vars)
  }
  def in_twice(implicit p: parsing.QueryParsers,
               expr: parsing.QueryParsers#Exp,
               in: parsing.QueryParsers#Exp) = macro_"$expr in ($in, $in)"
  def null_macros(b: QueryBuilder): Expr = null
  def dummy(b: QueryBuilder) = b.buildExpr("dummy")
  def dummy_table(b: QueryBuilder) = b.IdentExpr(List("dummy"))
  def macro_interpolator_test1(implicit p: parsing.QueryParsers,
                               e1: parsing.QueryParsers#Exp,
                               e2: parsing.QueryParsers#Exp) = macro_"($e1 + $e2)"
  def macro_interpolator_test2(implicit p: parsing.QueryParsers,
                               e1: parsing.QueryParsers#Exp,
                               e2: parsing.QueryParsers#Exp) =
    macro_"(macro_interpolator_test1($e1, $e1) + macro_interpolator_test1($e2, $e2))"
  def macro_interpolator_test3(implicit p: parsing.QueryParsers,
                               e1: parsing.QueryParsers#Exp,
                               e2: parsing.QueryParsers#Exp) =
    macro_"(macro_interpolator_test2($e1 * $e1, $e2 * $e2))"
  def macro_interpolator_test4(implicit p: parsing.QueryParsers,
                               table: parsing.QueryParsers#Exp,
                               col: parsing.QueryParsers#Exp) =
    macro_"$table { $table.$col }#(1)"
}

object Macros extends Macros