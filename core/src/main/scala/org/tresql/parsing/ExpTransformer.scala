package org.tresql.parsing

trait ExpTransformer { this: QueryParsers =>

  def transformer(fun: PartialFunction[Exp, Exp]): PartialFunction[Exp, Exp] = {
    lazy val transform_traverse = fun orElse traverse
    def tr(x: Any): Any = x match {case e: Exp => transform_traverse(e) case l: List[_] => l map tr case _ => x}
    lazy val traverse: PartialFunction[Exp, Exp] = {
      case e: Ident => e
      case e: Id => e
      case e: IdRef => e
      case e: Res => e
      case All => All
      case e: IdentAll => e
      case e: Variable => e
      case Null => Null
      case Fun(n, pars, d) => Fun(n, pars map tr, d)
      case UnOp(o, op) => UnOp(o, tr(op))
      case BinOp(o, lop, rop) => BinOp(o, tr(lop), tr(rop))
      case TerOp(lop, op1, mop, op2, rop) => TerOp(tr(lop), op1, tr(mop), op2, tr(rop))
      case In(lop, rop, not) => In(tr(lop), rop map tr, not)
      case Obj(t, a, j, o, n) => Obj(transform_traverse(t), a, tr(j).asInstanceOf[Join], o, n)
      case Join(d, j, n) => Join(d, tr(j), n)
      case Col(c, a, t) => Col(tr(c), a, t)
      case Cols(d, cols) => Cols(d, cols map (tr(_).asInstanceOf[Col]))
      case Grp(cols, hv) => Grp(cols map tr, tr(hv))
      case Ord(cols) => Ord(cols map (c=> (c._1, tr(c._2), c._3)))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        Query(objs map (tr(_).asInstanceOf[Obj]), tr(filters).asInstanceOf[Filters],
            tr(cols).asInstanceOf[Cols], tr(gr).asInstanceOf[Grp], tr(ord).asInstanceOf[Ord],
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
    transform_traverse
  }

  def extractor[T](fun: PartialFunction[(T, Exp), T]): PartialFunction[(T, Exp), T] = {
    val noExtractor: PartialFunction[(T, Exp), T] = {case (r: T, e: Exp) => r}
    def tr(r: T, x: Any): T = x match {
      case e: Exp => extract_traverse((r, e))
      case l: List[_] => l.foldLeft(r) { (fr, el) => tr(fr, el) }
      case _ => r
    }
    lazy val extract_traverse: PartialFunction[(T, Exp), T] = fun orElse noExtractor andThen {
      case (r: T, e: Exp) => e match {
        case _: Ident | _: Id | _: IdRef | _: Res | All | _: IdentAll | _: Variable | Null => r
        case Fun(_, pars, _) => tr(r, pars)
        case UnOp(_, operand) => tr(r, operand)
        case BinOp(_, lop, rop) => tr(tr(r, lop), rop)
        case In(lop, rop, _) => tr(tr(r, lop), rop)
        case Obj(t, _, j, _, _) => tr(tr(r, t), j)
        case Join(_, j, _) => tr(r, j)
        case Col(c, _, _) => tr(r, c)
        case Cols(_, cols) => tr(r, cols)
        case Grp(cols, hv) => tr(tr(r, cols), hv)
        case Ord(cols) => tr(r, cols)
        case Query(objs, filters, cols, gr, ord, off, lim) =>
          tr(tr(tr(tr(tr(tr(tr(r, objs), filters), cols), gr), ord), off), lim)
        case Insert(_, _, cols, vals) => tr(tr(r, cols), vals)
        case Update(_, _, filter, cols, vals) => tr(tr(tr(r, filter), cols), vals)
        case Delete(_, _, filter) => tr(r, filter)
        case Arr(els) => tr(r, els)
        case Filters(f) => tr(r, f)
        case Braces(expr) => tr(r, expr)
        //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
        case x: Exp => sys.error("Unknown expression: " + x)
      }
    }
    extract_traverse
  }

  def extract[T](exp: Exp, extractor: PartialFunction[Any, T]): List[T] = {
    var result = List[T]()
    var extract_collect_traverse: PartialFunction[Any, Any] = null
    val traverse: PartialFunction[Any, Any] = {
      case _: Ident | _: Id | _: IdRef | _: Res | All | _: IdentAll | _: Variable | Null | null =>
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
      case Query(objs, filters, cols, gr, ord, off, lim) =>
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
      case v @ Variable("?", _, _, _) =>
        bindIdx += 1; v copy bindIdx.toString
      case v: Variable => v
    }
    f
  }
}
