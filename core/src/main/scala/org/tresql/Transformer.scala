package org.tresql

trait Transformer extends PartialFunction[Expr, Expr] { this: QueryBuilder =>

  protected var transformer: PartialFunction[Expr, Expr] = {
    case e => e
  } 
  
  def isDefinedAt(e: Expr) = true
  
  def apply(e: Expr) = e match {
    case BinExpr(o, lop, rop) => BinExpr(o, transformer(lop), transformer(rop))
    case BracesExpr(b) => BracesExpr(transformer(b))
    case e: ConstExpr => e
    case _ => e
  }
  
}