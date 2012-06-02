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
    case _ => e.defaultSQL
  }
  
}