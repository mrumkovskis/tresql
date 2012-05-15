package org.tresql.dialects

import org.tresql.Expr
import org.tresql.QueryBuilder

object InsensitiveCmp extends (Expr => String) {

  private var accents = ("", "", Map[Char, Char](), null: Expr => String)

  private val cmp_i = new ThreadLocal[Boolean]
  cmp_i set false
  private val accFlag = new ThreadLocal[Boolean]
  accFlag set false
  private val caseFlag = new ThreadLocal[Boolean]
  caseFlag set false

  private val COL = "'[COL]'"

  def apply(acc: String, normalized: String, dialect: Expr => String = null) = {
    accents = (acc, normalized, (acc.toList.zip(normalized.toList).toMap),
      dialect)
    this
  }

  def apply(e: Expr) = e match {
    case e.builder.FunExpr(mode @ ("cmp_i" | "cmp_i_start" | "cmp_i_end" | "cmp_i_any" | "cmp_i_exact"),
      List(col, value)) => {
      cleanup()
      //set thread indication that cmp_i function is in the stack. This is checked in VarExpr pattern guard
      cmp_i set true
      //obtain value sql statement. acc, upp flags should be set if necessary
      val v = value.sql
      var (acc, upp) = (accFlag.get, caseFlag.get)
      val sql = QueryBuilder((if (!acc) "translate(" else "") +
        (if (!upp) "lower(" else "") +
        COL +
        (if (!upp) ")" else "") +
        (if (!acc) ", '" + accents._1 + "', '" + accents._2 + "')" else ""),
        e.builder.env).sql.replace(COL, col.sql) + " like " +
        (mode match {
          case "cmp_i" | "cmp_i_any" => "'%' || " + v + " || '%'"
          case "cmp_i_start" => v + " || '%'"
          case "cmp_i_end" => "'%' || " + v
          case _ => v
        })
      cleanup()
      sql
    }
    case v @ e.builder.VarExpr(_, _) if (cmp_i.get) => {
      var (acc, upp) = (false, false)
      v().toString.dropWhile(c => {
        if (!acc) acc = accents._3.contains(c)
        if (!upp) upp = c.isUpper
        !acc || !upp
      })
      accFlag set acc
      caseFlag set upp
      if (accents._4 == null) v.defaultSQL else accents._4(e)
    }
    case e.builder.FunExpr("translate", List(arg, from, to)) if (accents._4 == null) => {
      "translate(" + arg.sql + ", " + from.sql + ", " + to.sql + ")"
    }
    case e.builder.FunExpr("lower", List(arg)) if (accents._4 == null) => "lower(" + arg.sql + ")"
    case _ if (accents._4 == null) => e.defaultSQL
    case _ => accents._4(e)
  }

  private def cleanup() {
    accFlag set false
    caseFlag set false
    cmp_i set false
  }

}