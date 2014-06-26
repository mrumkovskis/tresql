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

  def extract[T](exp: String, extractor: PartialFunction[Any, T]): List[T] = {
    var result = List[T]()
    var extract_collect_traverse: PartialFunction[Any, Any] = null
    extract_collect_traverse = extractor andThen { case v: T => result ::= v } orElse {
      case _: Ident | _: Id | _: IdRef | _: Res | _: All | _: IdentAll | _: Variable | Null | null =>
      case l: List[_] => l foreach extract_collect_traverse
      case Fun(_, pars, _) => extract_collect_traverse(pars)
      case UnOp(_, operand) => extract_collect_traverse(operand)
      case BinOp(_, lop, rop) =>
        extract_collect_traverse(lop); extract_collect_traverse(rop)
      case In(lop, rop, _) =>
        extract_collect_traverse(lop); extract_collect_traverse(rop)
      case Obj(t, _, j, _, _) =>
        extract_collect_traverse(j); extract_collect_traverse(t)
      case Join(_, j, _) => extract_collect_traverse(j)
      case Col(c, _, _) => extract_collect_traverse(c)
      case Cols(_, cols) => extract_collect_traverse(cols)
      case Grp(cols, hv) =>
        extract_collect_traverse(cols); extract_collect_traverse(hv)
      case Ord(cols) => cols.foreach(c => extract_collect_traverse(c._2))
      case Query(objs, filters, cols, _, gr, ord, off, lim) =>
        extract_collect_traverse(objs)
        extract_collect_traverse(filters)
        extract_collect_traverse(cols)
        extract_collect_traverse(gr)
        extract_collect_traverse(ord)
        extract_collect_traverse(off)
        extract_collect_traverse(lim)
      case Insert(_, _, cols, vals) =>
        extract_collect_traverse(cols)
        extract_collect_traverse(vals)
      case Update(_, _, filter, cols, vals) =>
        extract_collect_traverse(filter)
        extract_collect_traverse(cols)
        extract_collect_traverse(vals)
      case Delete(_, _, filter) => extract_collect_traverse(filter)
      case Arr(els) => extract_collect_traverse(els)
      case Filters(f) => extract_collect_traverse(f)
      case Braces(expr) => extract_collect_traverse(expr)
      //for the security
      case x => sys.error("Unknown expression: " + x)
    }
    extract_collect_traverse(parseExp(exp))
    result reverse
  }

  def variableExtractor: PartialFunction[Any, Variable] = {
    var bindIdx = 0
    val f: PartialFunction[Any, Variable] = {
      case v @ Variable("?", _, _) =>
        bindIdx += 1; v copy bindIdx.toString
      case v: Variable => v
    }
    f
  }
}