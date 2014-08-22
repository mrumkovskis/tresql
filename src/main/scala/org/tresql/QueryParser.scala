package org.tresql

object QueryParser extends parsing.QueryMemParsers {

  def parseExp(expr: String): Any = {
    Env.cache.flatMap(_.get(expr)).getOrElse {
      try {
        intermediateResults.get.clear
        val e = phrase(exprList)(new scala.util.parsing.input.CharSequenceReader(expr)) match {
          case Success(r, _) => r
          case x => sys.error(x.toString)
        }
        Env.cache.map(_.put(expr, e))
        e
      } finally intermediateResults.get.clear
    }
  }
  
  def transformTresql(tresql: String, transformer: PartialFunction[Exp, Exp]): String =
    transform(parseExp(tresql).asInstanceOf[Exp], transformer) tresql
  
  def transform(exp: Exp, transformer: PartialFunction[Exp, Exp]): Exp = {
    var transform_traverse: PartialFunction[Exp, Exp] = null
    def tr(x: Any): Any = x match {case e: Exp => transform_traverse(e) case l: List[_] => l map tr case _ => x} 
    transform_traverse = transformer orElse {
      case e: Ident => e
      case e: Id => e
      case e: IdRef => e
      case e: Res => e
      case e: All => e
      case e: IdentAll => e
      case e: Variable => e
      case Null => Null
      case Fun(n, pars, d) => Fun(n, pars map tr, d)
      case UnOp(o, op) => UnOp(o, tr(op))
      case BinOp(o, lop, rop) => BinOp(o, tr(lop), tr(rop))
      case In(lop, rop, not) => In(tr(lop), rop map tr, not)
      case Obj(t, a, j, o, n) => Obj(tr(t), a, tr(j).asInstanceOf[Join], o, n)
      case Join(d, j, n) => Join(d, tr(j), n)
      case Col(c, a, t) => Col(tr(c), a, t)
      case Cols(d, cols) => Cols(d, cols map (tr(_).asInstanceOf[Col]))
      case Grp(cols, hv) => Grp(cols map tr, tr(hv))
      case Ord(cols) => Ord(cols map (c=> (c._1, tr(c._2), c._3)))
      case Query(objs, filters, cols, d, gr, ord, off, lim) =>
        Query(objs map (tr(_).asInstanceOf[Obj]), tr(filters).asInstanceOf[Filters],
            cols map (tr(_).asInstanceOf[Col]), d, tr(gr).asInstanceOf[Grp], tr(ord).asInstanceOf[Ord],
            tr(off), tr(lim))
      case Insert(t, a, cols, vals) => Insert(tr(t).asInstanceOf[Ident], a,
          cols map (tr(_).asInstanceOf[Col]), tr(vals))
      case Update(t, a, filter, cols, vals) => Update(tr(t).asInstanceOf[Ident], a,
          tr(filter).asInstanceOf[Arr], cols map (tr(_).asInstanceOf[Col]), tr(vals))
      case Delete(t, a, filter) => Delete(tr(t).asInstanceOf[Ident], a, tr(filter).asInstanceOf[Arr]) 
      case Arr(els) => Arr(els map tr)
      case Filters(f) => Filters(f map (tr(_).asInstanceOf[Arr]))
      case Braces(expr) => Braces(tr(expr))
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x) 
    }
    transform_traverse(exp)
  }
  
  def extractFrom[T](exp: String, extractor: PartialFunction[Any, T]): List[T] =
    extract(parseExp(exp).asInstanceOf[Exp], extractor)

  def extract[T](exp: Exp, extractor: PartialFunction[Any, T]): List[T] = {
    var result = List[T]()
    var extract_collect_traverse: PartialFunction[Any, Any] = null
    val traverse: PartialFunction[Any, Any] = {
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
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
      case _ => //don't throw an error since extractor can change expression.
    }
    val collect: PartialFunction[T, Any] = {
      case v: T =>
        result ::= v
        v
    }
    
    extract_collect_traverse = extractor andThen collect andThen traverse orElse traverse
    extract_collect_traverse(exp)
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