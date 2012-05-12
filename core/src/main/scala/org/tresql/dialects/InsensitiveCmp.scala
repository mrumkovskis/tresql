package org.tresql.dialects

import org.tresql.Expr
import org.tresql.QueryBuilder

object InsensitiveCmp extends (Expr => String) {

  private var accents = ("", "", Map[Char, Char]())

  private val accFlag = new ThreadLocal[Boolean]
  accFlag set false
  private val caseFlag = new ThreadLocal[Boolean]
  caseFlag set false

  def apply(acc: String, normalized: String) =
    accents = (acc, normalized, (acc.toLowerCase.toList.zip(normalized.toLowerCase.toList).toMap))

  def apply(e: Expr) = e match {
    case e.builder.FunExpr("cmp_i", List(col, value)) => {
      cleanup()
      val v = value.sql
      var (acc, upp) = (accFlag.get, caseFlag.get)
      (if(!acc) "translate(" else "") + 
      (if(!upp) "lower(" else "") + 
      col.sql + 
      (if(!upp) ")" else "") +
      (if(!acc) ", '" + accents._1 + "', '" + accents._2 + "')" else "") + " like " + v
    }
    case v@e.builder.VarExpr(_, _) => {
      var (acc, upp) = (false, false)
      v().toString.dropWhile(c => {
        if (!acc) acc = !accents._3.contains(c)
        if (!upp) upp = c.isUpper
        !acc || !upp
      })
      accFlag set acc
      caseFlag set upp
      v.sql
    }
    case _ => e.sql
  }

  private def cleanup() {
    accFlag set false
    caseFlag set false
  }

}