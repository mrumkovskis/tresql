package org.tresql.dialects

import org.tresql.Expr

object PostgreSQLDialect extends (Expr=>String) {

  def apply(e: Expr) = e match {
    case e.builder.BinExpr("-", lop, rop) =>
      lop.sql + (if (e.exprType.getSimpleName == "SelectExpr") " except " else " - ") + rop.sql
    case _ => e.defaultSQL
  }

}
