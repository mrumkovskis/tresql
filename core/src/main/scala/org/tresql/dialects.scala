package org.tresql

import org.tresql.ast.CompilerAst.ExprType

package object dialects {

  object ANSISQLDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = exec(e)._1
    def apply(e: Expr) = exec(e)._2
    private def exec(e: Expr) = {
      val b = e.builder
      e match {
        case b.FunExpr("case", pars, false, None, None) if pars.size > 1 => (true, pars.grouped(2).map(l =>
          if (l.size == 1) "else " + l.head.sql else "when " + l.head.sql + " then " + l(1).sql)
          .mkString("case ", " ", " end"))
        case _ => (false, "<none>")
      }
    }
  }

  val VariableNameDialect: CoreTypes.Dialect = {
    case v: QueryBuilder#VarExpr => v.defaultSQL match {
      case s if (s.indexWhere (_ == '?') match {
        case -1 => false
        case x => s.indexWhere (_ == '?', x + 1) match {
          case -1 => false
          case _ => true
        }}) => s"/*${v.fullName}[*/$s/*]${v.fullName}*/"
      case s => s + s"/*${v.fullName}*/"
    }
    case r: QueryBuilder#ResExpr => r.defaultSQL + s"/*${r.name}*/"
    case id: QueryBuilder#IdExpr => id.defaultSQL + s"/*#${id.seqName}*/"
    case idref: QueryBuilder#IdRefExpr => idref.defaultSQL + s"/*:#${idref.seqName}*/"
    case idrefid: ORT#IdRefIdExpr => idrefid.defaultSQL + s"/*:#${idrefid.idRefSeq}#${idrefid.idSeq}*/"
  }

  def commonDialect(vendor: String): CoreTypes.Dialect = {
    case f: QueryBuilder#FunExpr
      if f.name == "cast" && f.params.size == 2 && f.params(1).isInstanceOf[QueryBuilder#ConstExpr] =>
      s"cast(${f.params(0).sql} as ${f.builder.env.metadata.to_sql_type(vendor,
        String.valueOf(f.params(1).asInstanceOf[QueryBuilder#ConstExpr].value))})"
  }

  object HSQLRawDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = {
      val b = e.builder
      e match {
        case b.FunExpr("lower", _: List[_], false, None, None) => true
        case b.FunExpr("translate", List(_, b.ConstExpr(from: String),
          b.ConstExpr(to: String)), false, None, None) if from.length == to.length => true
        case b.FunExpr("nextval", List(b.ConstExpr(_)), false, None, None) => true
        case v: QueryBuilder#VarExpr if is_sql_array(v) => true
        case c: QueryBuilder#CastExpr => true
        case b.BinExpr("`~`", _, _) => true
        case b.SelectExpr(List(b.Table(b.ConstExpr(ast.Null), _, _, _, _, _)), _, _, _, _, _, _, _, _, _) => true
        case _ => false
      }
    }
    def apply(e: Expr) = {
      val b = e.builder
      (e: @unchecked) match {
        case b.FunExpr("lower", List(p), false, None, None) => "lcase(" + p.sql + ")"
        case b.FunExpr("translate", List(col, b.ConstExpr(from: String),
          b.ConstExpr(to: String)), false, None, None) if from.length == to.length =>
          (from zip to).foldLeft(col.sql)((s, a) => "replace(" + s + ", '" + a._1 + "', '" + a._2 + "')")
        case b.FunExpr("nextval", List(b.ConstExpr(seq)), false, None, None) => "next value for " + seq
        case v: QueryBuilder#VarExpr if is_sql_array(v) =>
          v.defaultSQL // register bind variable
          s"array[${sql_arr_bind_vars(v())}]"
        case c: QueryBuilder#CastExpr => s"cast(${c.exp.sql} as ${c.builder.env.to_sql_type("hsqldb", c.typ)})"
        case b.BinExpr("`~`", lop, rop) => s"regexp_matches(${lop.sql}, ${rop.sql})"
        case s @ b.SelectExpr(List(b.Table(b.ConstExpr(ast.Null), _, _, _, _, _)), _, _, _, _, _, _, _, _, _) =>
          s.copy(tables = List(s.tables.head.copy(table = b.IdentExpr(List("(values(0))"))))).sql
      }
    }
  }

  object OracleRawDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = {
      val b = e.builder
      e match {
        case b.ConstExpr(_: Boolean) => true
        case b.BinExpr("-", _, _) => true
        case _: QueryBuilder#TableColDefsExpr => true
        case e: QueryBuilder#SelectExpr if e.limit != null || e.offset != null => true
        case e: QueryBuilder#SelectExpr if e.cols.cols.headOption.exists(_.col match {
          case f: QueryBuilder#FunExpr if f.name == "optimizer_hint" && f.params.size == 1 &&
            f.params.head.isInstanceOf[QueryBuilder#ConstExpr] => true
          case _ => false
        }) => true
        case _ => false
      }
    }
    def apply(e: Expr) = {
      val b = e.builder
      e match {
        case b.ConstExpr(true) => "1 = 1"
        case b.ConstExpr(false) => "1 = 0"
        case b.TableColDefsExpr(_) => ""
        case b.FunExpr("optimizer_hint", List(b.ConstExpr(s: String)), false, None, None) => s
        case b.BinExpr("-", lop, rop) =>
          lop.sql + (if (e.exprType.getSimpleName == "SelectExpr") " minus " else " - ") + rop.sql
        case e: QueryBuilder#SelectExpr if e.limit != null || e.offset != null =>
          val b = e.builder //cannot match SelectExpr if builder is not extracted!!!
          e match {
            case s @ b.SelectExpr(_, _, _, _, _, _, o, l, _, _) =>
              val ns = s.copy(offset = null, limit = null)
              (o, l) match {
                case (null, null) => ns.sql
                case (null, l) =>
                  "select * from (" + ns.sql + ") where rownum <= " + l.sql
                case (o, null) =>
                  "select * from (select w.*, rownum rnum from (" + ns.sql + ") w) where rnum > " + o.sql
                case _ =>
                  "select * from (select w.*, rownum rnum from (" + ns.sql +
                    ") w where rownum <= " + l.sql + ") where rnum > " + o.sql
              }
            case x => sys.error(s"Unexpected case: $x")
          }
        case e: QueryBuilder#SelectExpr if e.cols.cols.headOption.exists(_.col match {
          case f: QueryBuilder#FunExpr if f.name == "optimizer_hint" && f.params.size == 1 &&
            f.params.head.isInstanceOf[QueryBuilder#ConstExpr] => true
          case _ => false
        }) =>
          val b = e.builder
          e match {
            case s @ b.SelectExpr(_, _,
              c @ b.ColsExpr(b.ColExpr(b.FunExpr(_, List(b.ConstExpr(h)), _, _, _), _, _, _) :: t, _, _, _, _),
              _, _, _, _, _, _, _) =>
                   "select " + String.valueOf(h) + (s.copy(cols = c.copy(cols = t)).sql substring 6)
          }
      }
    }
  }

  val PostgresqlRawDialect: CoreTypes.Dialect = {
    case c: QueryBuilder#ColExpr if c.alias != null => Option(c.col).map(_.sql).getOrElse("null") + " as " + c.alias
    case c: QueryBuilder#CastExpr => c.exp.sql + "::" + c.builder.env.metadata.to_sql_type("postgresql", c.typ)
    case v: QueryBuilder#VarExpr if is_sql_array(v) =>
      v.defaultSQL // register bind variable
      s"array[${sql_arr_bind_vars(v())}]"
    case f: QueryBuilder#FunExpr if f.name == "decode" && f.params.size > 2 =>
      f.params.tail.grouped(2).map { g =>
        if (g.size == 2) s"when ${g(0).sql} then ${g(1).sql}"
        else s"else ${g(0).sql}"
      }.mkString(s"case ${f.params(0).sql} ", " ", " end")
    case i: QueryBuilder#InsertExpr =>
      //pg insert as select needs column cast if bind variables are from 'from' clause select
      val b = i.builder
      i.vals match {
        case ivals@b.SelectExpr(
        List(valstable@b.Table(b.BracesExpr(vals: b.SelectExpr), _, _, _, _, _)),
        _, _, _, _, _, _, _, _, _) =>
          val table = i.table.name.last
          //insertable column names
          val colNames = i.cols.collect { case b.ColExpr(b.IdentExpr(name), _, _, _) => name.last } toSet
          //second level query which needs column casts matching insertable column names
          val colsWithCasts =
            vals.cols.cols.map {
              case c@b.ColExpr(e, a, _, _) if colNames(a) =>
                val md = c.builder.env.metadata
                md.colOption(table, a).map {
                  case org.tresql.metadata.Col(_, _, ExprType(tn)) => md.to_sql_type("postgresql", tn)
                }
                .map(typ => c.copy(col = b.CastExpr(e, typ)))
                .getOrElse(c)
              case x => x
            }
          //copy modified cols to second level query cols
          val colsExprWithCasts = vals.cols.copy(cols = colsWithCasts)
          new b.InsertExpr(i.table.asInstanceOf[b.IdentExpr], i.alias, i.cols, ivals
            .copy(tables = List(valstable.copy(
              table = b.BracesExpr(vals.copy(cols = colsExprWithCasts))))),
            i.insertConflict.asInstanceOf[b.InsertConflictExpr],
            i.returning.asInstanceOf[Option[b.ColsExpr]]
          ).defaultSQL
        case _ => i.defaultSQL
      }
  }

  def is_sql_array(variable: QueryBuilder#VarExpr): Boolean = !variable.allowArrBind && {
    val value = variable()
    !value.isInstanceOf[Array[Byte]] && value.isInstanceOf[Array[_]] || value.isInstanceOf[Iterable[_]]
  }
  def sql_arr_bind_vars(value: Any) = {
    def sql(i: Iterable[_]) = i.iterator.map(_ => "?").mkString(", ")
    value match {
      case a: Array[_] => sql(a.toSeq)
      case i: Iterable[_] => sql(i)
      case x => sys.error(s"Cannot make bind vars string from $x, iterable needed")
    }
  }

  def HSQLDialect: CoreTypes.Dialect =
    HSQLRawDialect orElse commonDialect("hsqldb") orElse ANSISQLDialect
  def OracleDialect: CoreTypes.Dialect =
    OracleRawDialect orElse commonDialect("oracle") orElse ANSISQLDialect
  def PostgresqlDialect: CoreTypes.Dialect =
    PostgresqlRawDialect orElse commonDialect("postgresql") orElse ANSISQLDialect
}
