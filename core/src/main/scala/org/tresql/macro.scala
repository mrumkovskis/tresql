package org.tresql

import parsing.{Arr, Const, Exp, QueryParsers}

import scala.util.Try

package object macro_ {
  implicit class TresqlMacroInterpolator(val sc: StringContext) extends AnyVal {
    def macro_(args: Exp*)(implicit p: QueryParsers): Exp = {
      p.parseExp(sc.standardInterpolator(StringContext.processEscapes, args.map(_.tresql)))
    }
  }
}

class Macros {
  def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr) = b.SQLExpr(String valueOf const.value, Nil)

  private def containsVar(b: QueryBuilder, v: QueryBuilder#VarExpr) =
    if (v.members != null && v.members.nonEmpty) b.env.contains(v.name, v.members)
    else b.env contains v.name

  def if_defined(b: QueryBuilder, v: Expr, e: Expr) = v match {
    case ve: QueryBuilder#VarExpr => if (containsVar(b, ve)) e else null
    case null => null
    case _ => e
  }

  def if_defined_or_else(b: QueryBuilder, v: Expr, e1: Expr, e2: Expr) =
    Option(if_defined(b, v, e1)).getOrElse(e2)

  def if_missing(b: QueryBuilder, v: Expr, e: Expr) = v match {
    case ve: QueryBuilder#VarExpr => if (containsVar(b, ve)) null else e
    case null => e
    case _ => null
  }

  def if_all_defined(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_all_defined macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars forall {
      case v: QueryBuilder#VarExpr => containsVar(b, v)
      case null => false
      case _ => true
    }) expr
    else null
  }

  def if_any_defined(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_any_defined macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars exists {
      case v: QueryBuilder#VarExpr => containsVar(b, v)
      case null => false
      case _ => true
    }) expr
    else null
  }

  def if_all_missing(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_all_missing macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars forall {
      case v: QueryBuilder#VarExpr => !containsVar(b, v)
      case null => true
      case _ => false
    }) expr
    else null
  }

  def if_any_missing(b: QueryBuilder, e: Expr*) = {
    if (e.size < 2) sys.error("if_any_missing macro must have at least two arguments")
    val vars = e dropRight 1
    val expr = e.last
    if (vars exists {
      case v: QueryBuilder#VarExpr => !containsVar(b, v)
      case null => true
      case _ => false
    }) expr
    else null
  }

  def sql_concat(b: QueryBuilder, exprs: Expr*) =
    b.SQLConcatExpr(exprs: _*)

  def ~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  def !~~ (b: QueryBuilder, lop: Expr, rop: Expr) =
    b.BinExpr("!~", b.FunExpr("lower", List(lop)), b.FunExpr("lower", List(rop)))

  /** Allows to specify table name as bind variable value.
   * Like {{{ []dynamic_table(:table)[deptno = 10]{dname} }}}
   * */
  def dynamic_table(b: QueryBuilder, table_name: QueryBuilder#VarExpr): b.Table = {
    b.Table(b.IdentExpr(List(String.valueOf(table_name()))), null, null, null, false, null)
  }

  /** Transforms expression passed in an argument as follows:
   *    1. Finds array expressions in an argument and checks whether all arrays are equal (otherwise throws assertion error)
   *    2. maps all elements in an array found to transformed expression which replaces array found with concrete element
   *    3. Returns resulting array expression
   *
   *    Examples:
   *      {{{map_exprs(parser, parser.parseExp("x || [a, b, c]")).tresql}}} returns
   *      {{{[x || a, x || b, x || c]}}}
   *
   *      {{{map_exprs(parser, parser.parseExp("fun(x || [a, b, c] || y, [a, b, c])")).tresql}}} returns
   *      {{{[fun(x || a || y, a), fun(x || b || y, b), fun(x || c || y, c)]}}}
   *
   *    NOTE: function may throw an error array element is transformed into the place where array is required,
   *          like in Values expresion for example.
   * */
  def map_exps(p: QueryParsers, exp: Exp): Exp = {
    val arrs = p.traverser[List[Arr]] (al => { case a: Arr => a :: al })(Nil)(exp)
    def mapArr(exps: List[Exp]) =
      exps.map { e => p.transformer { case _: Arr => e }(exp) }
    arrs match {
      case Nil => exp
      case List(Arr(els)) => Arr(mapArr(els))
      case Arr(els) :: tail =>
        require(tail.forall(els == _.elements),
          s"map_exps macro error. All arrays to be mapped in expression ($exp) must be equal!")
        Arr(mapArr(els))
    }
  }

  /** Similar to scala {{{list.mkString(start, sep, end)}}} method
   *  prefix, sep, postfix parameters must be string constants
   *  Resulting string must be parseable tresql expression
   * */
  def concat_exps(p: QueryParsers, prefix: Exp, sep: Exp, postfix: Exp, exprs: Exp*): Exp = {
    val (prefixStr: String, sepStr: String, postfixStr: String) = (prefix, sep, postfix) match {
      case (Const(pf), Const(sp), Const(psf)) => (pf.toString, sp.toString, psf.toString)
      case _ => new IllegalArgumentException(
        s"Prefix, separator, postfix parameters must be string constants, instead found: " +
          s"prefix - $prefix, sep - $sep, postfix - $postfix")
    }
    val exprs_to_concat = exprs match {
      case Seq(x: Arr) => x.elements
      case x => x
    }
    val expStr = exprs_to_concat.map(_.tresql).mkString(prefixStr, sepStr, postfixStr)
    Try(p.parseExp(expStr)).recover {
      case e: Exception => throw new IllegalArgumentException("concat_exps macro produced invalid tresql", e)
    }.get
  }

  /**
   *
   * Below ORT functionality macros
   *
   * */

  def _lookup_upsert(b: ORT,
                     objProp: QueryBuilder#ConstExpr,
                     idProp: QueryBuilder#ConstExpr,
                     lookupUpsertExpr: Expr,
                     idSelExpr: Expr) =
    b.LookupUpsertExpr(
      String valueOf objProp.value,
      String valueOf idProp.value,
      lookupUpsertExpr,
      idSelExpr
    )

  def _upsert(b: ORT, updateExpr: Expr, insertExpr: Expr) = b.UpsertExpr(updateExpr, insertExpr)

  def _delete_missing_children(b: ORT,
                               objName: QueryBuilder#ConstExpr,
                               key: QueryBuilder#ArrExpr,
                               keyValExprs: QueryBuilder#ArrExpr,
                               deleteExpr: Expr) =
    b.DeleteMissingChildrenExpr(
      String valueOf objName.value,
      key.elements.collect {
        case b.IdentExpr(n) => n.mkString(".")
        case x => sys.error(s"Unrecognized key type - $x")
      },
      keyValExprs.elements,
      deleteExpr)

  def _not_delete_keys(b: ORT, key: QueryBuilder#ArrExpr, keyValExprs: QueryBuilder#ArrExpr) =
    b.NotDeleteKeysExpr(key.elements, keyValExprs.elements)

  def _id_ref_id(b: ORT,
    idRef: QueryBuilder#IdentExpr,
    id: QueryBuilder#IdentExpr) =
    b.IdRefIdExpr(idRef.name.mkString("."), id.name.mkString("."))

  def _id_by_key(b: ORT, idExpr: Expr) = b.IdByKeyExpr(idExpr)

  def _update_by_key(b: ORT, table: QueryBuilder#IdentExpr, setIdExpr: Expr, updateExpr: Expr) = {
    b.UpdateByKeyExpr(table.name.mkString("."), setIdExpr, updateExpr)
  }

  def _deferred_build(b: ORT, exp: Exp) = {
    b.DeferredBuildExpr(exp)
  }
}
