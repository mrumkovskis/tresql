package org.tresql

object QueryParser extends parsing.QueryMemParsers {

  def parseExp(expr: String): Any = {
    Env.cache.flatMap(_.get(expr)).getOrElse {
      intermediateResults.value.clear
      val e = phrase(exprList)(new lexical.Scanner(expr)) match {
        case Success(r, _) => r
        case x => sys.error(x.toString)
      }
      Env.cache.map(_.put(expr, e))
      e
    }
  }

  def bindVariables(ex: String): List[String] = {
    var bindIdx = 0
    val vars = scala.collection.mutable.ListBuffer[String]()
    def bindVars(parsedExpr: Any): Any =
      parsedExpr match {
        case _: Ident | _: Id | _: IdRef | _: Res | _: All | _: IdentAll | Null | null =>
        case l: List[_] => l foreach bindVars
        case Variable("?", _) =>
          bindIdx += 1; vars += bindIdx.toString
        case Variable(n, _) => vars += n
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
        case Ord(cols) => cols.foreach(c=> bindVars(c._2))
        case Query(objs, filters, cols, _, gr, ord, _, _) => {
          bindVars(objs)
          bindVars(filters)
          bindVars(cols)
          bindVars(gr)
          bindVars(ord)
        }
        case Insert(_, _, cols, vals) => {
          bindVars(cols)
          bindVars(vals)
        }
        case Update(_, _, filter, cols, vals) => {
          bindVars(filter)
          bindVars(cols)
          bindVars(vals)
        }
        case Delete(_, _, filter) => bindVars(filter)
        case Arr(els) => bindVars(els)
        case Filters(f) => bindVars(f)
        case Braces(expr) => bindVars(expr)
        //for the security
        case x => sys.error("Unknown expression: " + x)
      }
    parseExp(ex) match {
      case Success(r, _) => {
        bindVars(r)
        vars.toList
      }
      case x => sys.error(x.toString)
    }

  }
}