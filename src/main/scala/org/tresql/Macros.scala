package org.tresql

class Macros {
  def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr) = b.SQLExpr(String valueOf const.value, Nil)

  def ~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def !~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("!~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def _lookup_edit(b: QueryBuilder,
    obj: QueryBuilder#ConstExpr,
    pk: QueryBuilder#ConstExpr,
    insertExpr: Expr,
    updateExpr: Expr) =
      b.LookupEditExpr(
        String valueOf obj.value,
        if (pk.value == null) null else String valueOf pk.value,
        insertExpr, updateExpr)
}
