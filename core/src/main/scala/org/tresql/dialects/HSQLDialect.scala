package org.tresql.dialects

import org.tresql.Expr

object HSQLDialect extends (Expr=>String) {

  def apply(e:Expr) = e match {
    case e.builder.FunExpr("lower", List(p)) => "lcase(" + p.sql + ")"
    case e.builder.FunExpr("translate", List(col, e.builder.ConstExpr(from: String), 
        e.builder.ConstExpr(to: String))) if (from.length == to.length) => {
          (from zip to).foldLeft(col.sql)((s, a) => "replace(" + s + ", '" + a._1 + "', '" + a._2 + "')")
        }
    case e.builder.FunExpr("nextval", List(e.builder.ConstExpr(seq))) => "next value for " + seq
    case e.builder.FunExpr("case", pars) if pars.size > 1 => pars.grouped(2).map(l=>
      if(l.size == 1) "else " + l(0).sql else "when " + l(0).sql + " then " + l(1).sql)
      .mkString("case ", " ", " end")
    case _ => e.defaultSQL
  }
  
}