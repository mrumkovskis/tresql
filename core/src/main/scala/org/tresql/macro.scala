package org.tresql

import scala.annotation.tailrec

package object macro_ {
  implicit class TresqlMacroInterpolator(val sc: StringContext) extends AnyVal {
    def macro_(args: Expr*)(implicit b: QueryBuilder): Expr = {
      def unusedName(name: String, usedNames: collection.Set[String]): String = {
        @tailrec
        def unusedName(index: Int): String =
          if (!(usedNames contains (name + index))) name + index
          else unusedName(index + 1)
        if (!usedNames.contains(name)) name else unusedName(2)
      }
      var usedNames: Set[String] = Set()
      b.transform(b.buildExpr(sc.standardInterpolator(identity, args.map(a => ":x"))), {
        case v @ b.VarExpr(name, _, _) => usedNames += name; v
      })
      val zipped = args.zipWithIndex.map { kv =>
        val name = unusedName(s"_arg_${kv._2}_", usedNames)
        usedNames += name
        name -> kv._1
      }
      val params = zipped.toMap
      val expr = sc.standardInterpolator(identity, zipped.map(":" + _._1))
      b.transform(b.buildExpr(expr), {
        case v @ b.VarExpr(name, _, _) =>
          params.getOrElse(name, v)
      })
    }
  }
}

class Macros {
  def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr) = b.SQLExpr(String valueOf const.value, Nil)

  def if_defined(b: QueryBuilder, v: QueryBuilder#VarExpr, e: Expr) =
    if (v != null && (b.env contains v.name)) e else null

  def if_missing(b: QueryBuilder, v: QueryBuilder#VarExpr, e: Expr) =
    if (v != null && (b.env contains v.name)) null else e

  def if_all_defined(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_all_defined macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars forall {
      case v: QueryBuilder#VarExpr => b.env contains v.name
      case null => false
      case x => sys.error(s"Unexpected parameter type in if_all_defined macro, expecting VarExpr: $x")
    }) expr
    else null
  }

  def if_any_defined(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_any_defined macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars exists {
      case v: QueryBuilder#VarExpr => b.env contains v.name
      case null => false
      case x => sys.error(s"Unexpected parameter type in if_any_defined macro, expecting VarExpr: $x")
    }) expr
    else null
  }

  def if_all_missing(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_all_missing macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars forall {
      case v: QueryBuilder#VarExpr => !(b.env contains v.name)
      case null => true
      case x => sys.error(s"Unexpected parameter type in if_all_missing macro, expecting VarExpr: $x")
    }) expr
    else null
  }

  def if_any_missing(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_any_missing macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars exists {
      case v: QueryBuilder#VarExpr => !(b.env contains v.name)
      case null => true
      case x => sys.error(s"Unexpected parameter type in if_any_missing macro, expecting VarExpr: $x")
    }) expr
    else null
  }

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

  def _upsert(b: ORT, updateExpr: Expr, insertExpr: Expr) = b.UpsertExpr(updateExpr, insertExpr)

  def _delete_children(b: ORT,
    objName: QueryBuilder#ConstExpr,
    tableName: QueryBuilder#ConstExpr,
    deleteExpr: Expr) = b.DeleteChildrenExpr(
      String valueOf objName.value,
      String valueOf tableName.value,
      deleteExpr)

  def _not_delete_ids(b: ORT, idsExpr: Expr) = b.NotDeleteIdsExpr(idsExpr)

  def _id_ref_id(b: ORT,
    idRef: QueryBuilder#IdentExpr,
    id: QueryBuilder#IdentExpr) =
    b.IdRefIdExpr(idRef.name.mkString("."), id.name.mkString("."))
}
