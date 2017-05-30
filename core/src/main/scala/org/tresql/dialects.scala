package org.tresql

package object dialects {

  object ANSISQLDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = exec(e)._1
    def apply(e: Expr) = exec(e)._2
    private def exec(e: Expr) = {
      val b = e.builder
      e match {
        case b.FunExpr("case", pars, false) if pars.size > 1 => (true, pars.grouped(2).map(l =>
          if (l.size == 1) "else " + l(0).sql else "when " + l(0).sql + " then " + l(1).sql)
          .mkString("case ", " ", " end"))
        case _ => (false, "<none>")
      }
    }
  }

  object HSQLRawDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = {
      val b = e.builder
      e match {
        case b.FunExpr("lower", _: List[_], false) => true
        case b.FunExpr("translate", List(_, b.ConstExpr(from: String),
          b.ConstExpr(to: String)), false) if (from.length == to.length) => true
        case b.FunExpr("nextval", List(b.ConstExpr(_)), false) => true
        case _ => false
      }
    }
    def apply(e: Expr) = {
      val b = e.builder
      (e: @unchecked) match {
        case b.FunExpr("lower", List(p), false) => "lcase(" + p.sql + ")"
        case b.FunExpr("translate", List(col, b.ConstExpr(from: String),
          b.ConstExpr(to: String)), false) if (from.length == to.length) => {
          (from zip to).foldLeft(col.sql)((s, a) => "replace(" + s + ", '" + a._1 + "', '" + a._2 + "')")
        }
        case b.FunExpr("nextval", List(b.ConstExpr(seq)), false) => "next value for " + seq
      }
    }
  }

  object OracleRawDialect extends CoreTypes.Dialect {
    def isDefinedAt(e: Expr) = {
      val b = e.builder
      e match {
        case b.BinExpr("-", _, _) => true
        case e: QueryBuilder#SelectExpr if e.limit != null || e.offset != null => true
        case e: QueryBuilder#SelectExpr if e.cols.cols.headOption.map {
          _.col match {
            case f: QueryBuilder#FunExpr if f.name == "optimizer_hint" && f.params.size == 1 &&
              f.params.head.isInstanceOf[QueryBuilder#ConstExpr] => true
            case _ => false
          }
        } getOrElse false => true
        case _ => false
      }
    }
    def apply(e: Expr) = {
      val b = e.builder
      e match {
        case b.ConstExpr(true) => "'Y'"
        case b.ConstExpr(false) => "'N'"
        case b.FunExpr("optimizer_hint", List(b.ConstExpr(s: String)), false) => s
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
        case e: QueryBuilder#SelectExpr if e.cols.cols.headOption.map {
          _.col match {
            case f: QueryBuilder#FunExpr if f.name == "optimizer_hint" && f.params.size == 1 &&
              f.params.head.isInstanceOf[QueryBuilder#ConstExpr] => true
            case _ => false
          }
        } getOrElse false =>
          val b = e.builder
          e match {
            case s @ b.SelectExpr(_, _,
              c @ b.ColsExpr(b.ColExpr(b.FunExpr(_, List(b.ConstExpr(h)), _), _, _, _, _) :: t, _, _, _),
              _, _, _, _, _, _, _) =>
                   "select " + String.valueOf(h) + (s.copy(cols = c.copy(cols = t)).sql substring 6)
          }
      }
    }
  }

  val PostgresqlRawDialect: CoreTypes.Dialect = {
    case c: QueryBuilder#ColExpr if c.alias != null => c.col.sql + " as " + c.alias
    case v: QueryBuilder#VarExpr if v.typ != null => s"cast(${v.defaultSQL} as ${v.typ})"
  }

  def HSQLDialect = HSQLRawDialect orElse ANSISQLDialect

  def OracleDialect = OracleRawDialect orElse ANSISQLDialect

  def PostgresqlDialect = PostgresqlRawDialect orElse ANSISQLDialect

}
