package org.tresql

object QueryParser extends parsing.QueryMemParsers {

  def parseExp(expr: String): Any = {
    Env.cache.flatMap(_.get(expr)).getOrElse {
      try {
        intermediateResults.get.clear
        val e = phrase(exprList)(new lexical.Scanner(expr)) match {
          case Success(r, _) => r
          case x => sys.error(x.toString)
        }
        Env.cache.map(_.put(expr, e))
        e
      } finally intermediateResults.get.clear
    }
  }

  def bindVariables(ex: String): List[Variable] = {
    var bindIdx = 0
    val vars = scala.collection.mutable.ListBuffer[Variable]()
    def bindVars(parsedExpr: Any): Any =
      parsedExpr match {
        case _: Ident | _: Id | _: IdRef | _: Res | _: All | _: IdentAll | Null | null =>
        case l: List[_] => l foreach bindVars
        case v @ Variable("?", _, _) =>
          bindIdx += 1; vars += v copy bindIdx.toString
        case v @ Variable(_, _, _) => vars += v
        case Fun(_, pars, _) => bindVars(pars)
        case UnOp(_, operand) => bindVars(operand)
        case BinOp(_, lop, rop) =>
          bindVars(lop); bindVars(rop)
        case In(lop, rop, _) =>
          bindVars(lop); bindVars(rop)
        case Obj(t, _, j, _, _) =>
          bindVars(j); bindVars(t)
        case Join(_, j, _) => bindVars(j)
        case Col(c, _, _) => bindVars(c)
        case Cols(_, cols) => bindVars(cols)
        case Grp(cols, hv) =>
          bindVars(cols); bindVars(hv)
        case Ord(cols) => cols.foreach(c => bindVars(c._2))
        case Query(objs, filters, cols, _, gr, ord, off, lim) =>
          bindVars(objs)
          bindVars(filters)
          bindVars(cols)
          bindVars(gr)
          bindVars(ord)
          bindVars(off)
          bindVars(lim)
        case Insert(_, _, cols, vals) =>
          bindVars(cols)
          bindVars(vals)
        case Update(_, _, filter, cols, vals) =>
          bindVars(filter)
          bindVars(cols)
          bindVars(vals)
        case Delete(_, _, filter) => bindVars(filter)
        case Arr(els) => bindVars(els)
        case Filters(f) => bindVars(f)
        case Braces(expr) => bindVars(expr)
        //for the security
        case x => sys.error("Unknown expression: " + x)
      }
    bindVars(parseExp(ex))
    vars.toList
  }
}