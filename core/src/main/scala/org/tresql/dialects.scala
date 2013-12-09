package org.tresql

package object dialects {

  object ANSISQLDialect extends PartialFunction[Expr, String] {
    def isDefinedAt(e: Expr) = exec(e)._1
    def apply(e: Expr) = exec(e)._2
    private def exec(e: Expr) = e match {
      case e.builder.FunExpr("case", pars, false) if pars.size > 1 => (true, pars.grouped(2).map(l =>
        if (l.size == 1) "else " + l(0).sql else "when " + l(0).sql + " then " + l(1).sql)
        .mkString("case ", " ", " end"))
      case _ => (false, "<none>")
    }
  }

  object HSQLRawDialect extends PartialFunction[Expr, String] {
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

  object OracleRawDialect extends PartialFunction[Expr, String] {
    def isDefinedAt(e: Expr) = e match {
      case e.builder.BinExpr("-", _, _) => true
      case e: QueryBuilder#SelectExpr if e.limit != null || e.offset != null => true
      case _ => false
    }
    def apply(e: Expr) = e match {
      case e.builder.ConstExpr(true) => "'Y'"
      case e.builder.ConstExpr(false) => "'N'"
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
    }
  }

  def HSQLDialect = HSQLRawDialect orElse ANSISQLDialect

  def OracleDialect = OracleRawDialect orElse ANSISQLDialect

  def InsensitiveCmp(accents: String, normalized: String): PartialFunction[Expr, String] =
    new InsensitiveCmpObj(accents, normalized)

}

package dialects {
  object InsensitiveCmpObj {
    private val cmp_i = new ThreadLocal[Boolean]
    cmp_i set false
    private val accFlag = new ThreadLocal[Boolean]
    accFlag set false
    private val caseFlag = new ThreadLocal[Boolean]
    caseFlag set false
  }

  class InsensitiveCmpObj(accents: String, normalized: String) extends PartialFunction[Expr, String] {
    import InsensitiveCmpObj._
    private val accentMap = accents.zip(normalized).toMap
    private val COL = "'[COL]'"

    def isDefinedAt(e: Expr) = e match {
      case e.builder.FunExpr(mode @ ("cmp_i" | "cmp_i_start" | "cmp_i_end" | "cmp_i_any" | "cmp_i_exact"),
        List(col, value), false) => true
      case v @ e.builder.VarExpr(_, _) if (cmp_i.get) => {
        var (acc, upp) = (false, false)
        v() match {
          case x if x != null => {
            x.toString.dropWhile(c => {
              if (!acc) acc = accentMap.contains(c)
              if (!upp) upp = c.isUpper
              !acc || !upp
            })
            accFlag set acc
            caseFlag set upp
            false
          }
          case _ => false
        }
      }
      case _ => false
    }

    def apply(e: Expr) = (e: @unchecked) match {
      case e.builder.FunExpr(mode @ ("cmp_i" | "cmp_i_start" | "cmp_i_end" | "cmp_i_any" | "cmp_i_exact"),
        List(col, value), false) => {
        cleanup()
        try {
          //set thread indication that cmp_i function is in the stack. This is checked in VarExpr pattern guard
          cmp_i set true
          //obtain value sql statement. acc, upp flags should be set if necessary
          val v = value.sql
          var (acc, upp) = (accFlag.get, caseFlag.get)
          val sql = QueryBuilder((if (!acc) "translate(" else "") +
            (if (!upp) "lower(" else "") +
            COL +
            (if (!upp) ")" else "") +
            (if (!acc) ", '" + accents + "', '" + normalized + "')" else ""),
            e.builder.env).sql.replace(COL, col.sql) + " like " +
            (mode match {
              case "cmp_i" | "cmp_i_any" => "'%' || " + v + " || '%'"
              case "cmp_i_start" => v + " || '%'"
              case "cmp_i_end" => "'%' || " + v
              case _ => v
            })
          sql
        } finally {
          cleanup()
        }
      }
    }

    private def cleanup() {
      accFlag set false
      caseFlag set false
      cmp_i set false
    }

  }
}