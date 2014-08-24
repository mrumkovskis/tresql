package org.tresql

class Macros {
  def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr) = b.SQLExpr(String valueOf const.value, Nil)
  
  def ~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("~", b.FunExpr("upper", List(lop)), b.FunExpr("upper", List(rop)))

  def !~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("!~", b.FunExpr("upper", List(lop)), b.FunExpr("upper", List(rop)))
 
}