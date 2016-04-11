package org.tresql

package object dialects {

  object ANSISQLDialect extends Dialect {
    def isDefinedAt(e: Expr) = exec(e)._1
    def apply(e: Expr) = exec(e)._2
    private def exec(e: Expr) = e match {
      case e.builder.FunExpr("case", pars, false) if pars.size > 1 => (true, pars.grouped(2).map(l =>
        if (l.size == 1) "else " + l(0).sql else "when " + l(0).sql + " then " + l(1).sql)
        .mkString("case ", " ", " end"))
      case _ => (false, "<none>")
    }
  }

  object HSQLRawDialect extends Dialect {
    def isDefinedAt(e: Expr) = e match {
      case e.builder.FunExpr("lower", _: List[_], false) => true
      case e.builder.FunExpr("translate", List(_, e.builder.ConstExpr(from: String),
        e.builder.ConstExpr(to: String)), false) if (from.length == to.length) => true
      case e.builder.FunExpr("nextval", List(e.builder.ConstExpr(_)), false) => true
      case _ => false
    }
    def apply(e: Expr) = (e: @unchecked) match {
      case e.builder.FunExpr("lower", List(p), false) => "lcase(" + p.sql + ")"
      case e.builder.FunExpr("translate", List(col, e.builder.ConstExpr(from: String),
        e.builder.ConstExpr(to: String)), false) if (from.length == to.length) => {
        (from zip to).foldLeft(col.sql)((s, a) => "replace(" + s + ", '" + a._1 + "', '" + a._2 + "')")
      }
      case e.builder.FunExpr("nextval", List(e.builder.ConstExpr(seq)), false) => "next value for " + seq
    }
  }

  object OracleRawDialect extends Dialect {
    def isDefinedAt(e: Expr) = e match {
      case e.builder.BinExpr("-", _, _) => true
      case e: QueryBuilder#SelectExpr if e.limit != null || e.offset != null => true
      case e: QueryBuilder#SelectExpr if e.cols.headOption.map {
        _.col match {
          case f: QueryBuilder#FunExpr if f.name == "optimizer_hint" && f.params.size == 1 &&
            f.params.head.isInstanceOf[QueryBuilder#ConstExpr] => true
          case _ => false
        }
      } getOrElse false => true
      case _ => false
    }
    def apply(e: Expr) = e match {
      case e.builder.ConstExpr(true) => "'Y'"
      case e.builder.ConstExpr(false) => "'N'"
      case e.builder.FunExpr("optimizer_hint", List(e.builder.ConstExpr(s: String)), false) => s
      case e.builder.BinExpr("-", lop, rop) =>
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
        }
      case e: QueryBuilder#SelectExpr if e.cols.headOption.map {
        _.col match {
          case f: QueryBuilder#FunExpr if f.name == "optimizer_hint" && f.params.size == 1 &&
            f.params.head.isInstanceOf[QueryBuilder#ConstExpr] => true
          case _ => false
        }
      } getOrElse false =>
        val b = e.builder
        e match {
          case s @ b.SelectExpr(_, _,
              b.ColExpr(b.FunExpr(_, List(b.ConstExpr(h)), _), _, _, _, _) :: t, _, _, _, _, _, _, _) =>
                 "select " + String.valueOf(h) + (s.copy(cols = t).sql substring 6)
        }
    }
  }

  def HSQLDialect = HSQLRawDialect orElse ANSISQLDialect

  def OracleDialect = OracleRawDialect orElse ANSISQLDialect

}
