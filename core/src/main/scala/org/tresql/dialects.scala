package org.tresql

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
        }}) => s"/*${v.name}[*/$s/*]${v.name}*/"
      case s => s + s"/*${v.name}*/"
    }
    case r: QueryBuilder#ResExpr => r.defaultSQL + s"/*${r.name}*/"
  }

  val CommonDialect: CoreTypes.Dialect = {
    case f: QueryBuilder#FunExpr
      if f.name == "cast" && f.params.size == 2 && f.params(1).isInstanceOf[QueryBuilder#ConstExpr] =>
      s"cast(${f.params(0).sql} as ${f.params(1).asInstanceOf[QueryBuilder#ConstExpr].value})"
  }

  object HSQLRawDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = {
      val b = e.builder
      e match {
        case b.FunExpr("lower", _: List[_], false, None, None) => true
        case b.FunExpr("translate", List(_, b.ConstExpr(from: String),
          b.ConstExpr(to: String)), false, None, None) if from.length == to.length => true
        case b.FunExpr("nextval", List(b.ConstExpr(_)), false, None, None) => true
        case b.BinExpr("`~`", _, _) => true
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
        case b.BinExpr("`~`", lop, rop) => s"regexp_matches(${lop.sql}, ${rop.sql})"
      }
    }
  }

  object OracleRawDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = {
      val b = e.builder
      e match {
        case b.BinExpr("-", _, _) => true
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
              c @ b.ColsExpr(b.ColExpr(b.FunExpr(_, List(b.ConstExpr(h)), _, _, _), _, _, _) :: t, _, _, _),
              _, _, _, _, _, _, _) =>
                   "select " + String.valueOf(h) + (s.copy(cols = c.copy(cols = t)).sql substring 6)
          }
      }
    }
  }

  val PostgresqlRawDialect: CoreTypes.Dialect = {
    case c: QueryBuilder#ColExpr if c.alias != null => c.col.sql + " as " + c.alias
    case c: QueryBuilder#CastExpr => c.exp.sql + "::" + (c.typ match {
      case "string" => "text"
      case "long" => "bigint"
      case x => x
    })
  }

  def HSQLDialect = HSQLRawDialect orElse CommonDialect orElse ANSISQLDialect

  def OracleDialect = OracleRawDialect orElse CommonDialect orElse ANSISQLDialect

  def PostgresqlDialect = PostgresqlRawDialect orElse CommonDialect orElse ANSISQLDialect

}
