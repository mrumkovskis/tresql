package org.tresql

class Macros {
  def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr) = b.SQLExpr(String valueOf const.value, Nil)

  def if_defined(b: QueryBuilder, v: QueryBuilder#VarExpr, e: Expr) =
    if (b.env contains v.name) e else null

  def if_missing(b: QueryBuilder, v: QueryBuilder#VarExpr, e: Expr) =
    if (b.env contains v.name) null else e

  def if_all_defined(b: QueryBuilder, a: QueryBuilder#ArrExpr, e: Expr) =
    if (a.elements forall {
      case v: QueryBuilder#VarExpr => b.env contains v.name
      case x => sys.error(s"Unexpected parameter type in if_all_defined macro, expecting VarExpr: $x")
    }) e
    else null

  def if_any_defined(b: QueryBuilder, a: QueryBuilder#ArrExpr, e: Expr) =
    if (a.elements exists {
      case v: QueryBuilder#VarExpr => b.env contains v.name
      case x => sys.error(s"Unexpected parameter type in if_any_defined macro, expecting VarExpr: $x")
    }) e
    else null

  def sql_concat(b: QueryBuilder, exprs: Expr*) =
    b.SQLConcatExpr(exprs: _*)

  def ~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def !~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("!~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def _lookup_edit(b: ORT,
    objName: QueryBuilder#ConstExpr,
    idName: QueryBuilder#ConstExpr,
    insertExpr: Expr,
    updateExpr: Expr) =
      b.LookupEditExpr(
        String valueOf objName.value,
        if (idName.value == null) null else String valueOf idName.value,
        insertExpr, updateExpr)

  def _insert_or_update(b: ORT,
    table: QueryBuilder#ConstExpr, insertExpr: Expr, updateExpr: Expr) =
    b.InsertOrUpdateExpr(String valueOf table.value, insertExpr, updateExpr)

  def _delete_children(b: ORT,
    objName: QueryBuilder#ConstExpr,
    tableName: QueryBuilder#ConstExpr,
    deleteExpr: Expr) = b.DeleteChildrenExpr(
      String valueOf objName.value,
      String valueOf tableName.value,
      deleteExpr)

  def _id_ref_id(b: ORT,
    idRef: QueryBuilder#IdentExpr,
    id: QueryBuilder#IdentExpr) =
    b.IdRefIdExpr(idRef.name.mkString("."), id.name.mkString("."))
}
