package org.tresql.dialects

import org.tresql.Expr
import org.tresql.QueryBuilder

object InsensitiveCmp extends (Expr => String) {

  private var accents = ("", "", Map[Char, Char]())

  private val cmp_i = new ThreadLocal[Boolean]
  cmp_i set false
  private val accFlag = new ThreadLocal[Boolean]
  accFlag set false
  private val caseFlag = new ThreadLocal[Boolean]
  caseFlag set false

  def apply(acc: String, normalized: String) = {
    accents = (acc, normalized, (acc.toLowerCase.toList.zip(normalized.toLowerCase.toList).toMap))
    this
  }

  def apply(e: Expr) = e match {
    case e.builder.FunExpr(mode@("cmp_i" | "cmp_i_start" | "cmp_i_end" | "cmp_i_any" | "cmp_i_exact"),
        List(col, value)) => {
      cleanup()
      cmp_i set true
      val v = value.sql
      var (acc, upp) = (accFlag.get, caseFlag.get)
      val sql = (if(!acc) "translate(" else "") + 
      (if(!upp) "lower(" else "") + 
      col.sql + 
      (if(!upp) ")" else "") +
      (if(!acc) ", '" + accents._1 + "', '" + accents._2 + "')" else "") + " like " + (mode match {
        case "cmp_i" | "cmp_i_any" => "'%' || " + v + " || '%'"
        case "cmp_i_start" => v + " || '%'"
        case "cmp_i_end" => "'%' || " + v
        case _ => v
      })
      cleanup()
      println("SQL: " + sql + " " + acc + upp)
      sql
    }
    case v@e.builder.VarExpr(_, _) if(cmp_i.get) => {
      var (acc, upp) = (false, false)
      v().toString.dropWhile(c => {
        if (!acc) acc = accents._3.contains(c)
        if (!upp) upp = c.isUpper
        !acc || !upp
      })
      accFlag set acc
      caseFlag set upp
      v.defaultSQL
    }
    case _ => e.defaultSQL
  }

  private def cleanup() {
    accFlag set false
    caseFlag set false
    cmp_i set false
  }

}