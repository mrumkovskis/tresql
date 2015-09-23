package org.tresql

class Macros {
  def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr) = b.SQLExpr(String valueOf const.value, Nil)

  def ~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def !~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("!~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def _lookup_edit(b: QueryBuilder,
    objName: QueryBuilder#ConstExpr,
    idName: QueryBuilder#ConstExpr,
    insertExpr: Expr,
    updateExpr: Expr) =
      b.LookupEditExpr(
        String valueOf objName.value,
        if (idName.value == null) null else String valueOf idName.value,
        insertExpr, updateExpr)
  def _insert_or_update(b: QueryBuilder,
    idName: String, insertExpr: Expr, updateExpr: Expr) =
    b.InsertOrUpdateExpr(idName, insertExpr, updateExpr)
  def _delete_children(b: QueryBuilder,
    objName: QueryBuilder#ConstExpr,
    idName: QueryBuilder#ConstExpr,
    deleteExpr: Expr) = b.DeleteChildrenExpr(
      String valueOf objName.value,
      String valueOf idName.value,
      deleteExpr)
}
