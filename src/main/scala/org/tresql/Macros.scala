package org.tresql

class Macros {
  def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr) = b.SQLExpr(String valueOf const.value, Nil)
  
  def ~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def !~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("!~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))
    
  def _changeEnv(b: QueryBuilder, key: QueryBuilder#ConstExpr, expr: Expr) = 
    b.ChangeEnvExpr(String valueOf key.value, expr)

  def _lookupInsert(b: QueryBuilder, key: QueryBuilder#ConstExpr, expr: Expr) =
    b.LookupInsertExpr(String valueOf key.value, expr)
}
