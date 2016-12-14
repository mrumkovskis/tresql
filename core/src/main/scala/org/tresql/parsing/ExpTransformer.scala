package org.tresql.parsing

trait ExpTransformer { this: QueryParsers =>

  type Transformer = PartialFunction[Exp, Exp]
  type Extractor[T] = PartialFunction[(T, Exp), T]
  type ExtractorAndTraverser[T] = PartialFunction[(T, Exp), (T, Boolean)]

  def transformer(fun: Transformer): Transformer = {
    lazy val transform_traverse = fun orElse traverse
    def tr(x: Any): Any = x match {
      case e: Exp @unchecked => transform_traverse(e)
      case l: List[_] => l map tr
      case _ => x
    }
    lazy val traverse: Transformer = {
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
      case WithTable(n, c, r, q) => WithTable(n, c, r, tr(q).asInstanceOf[Exp])
      case With(ts, q) => With(tr(ts).asInstanceOf[List[WithTable]], tr(q).asInstanceOf[Exp])
      case Insert(t, a, cols, vals) => Insert(tr(t).asInstanceOf[Ident], a,
          tr(cols).asInstanceOf[List[Col]], tr(vals))
      case Update(t, a, filter, cols, vals) => Update(tr(t).asInstanceOf[Ident], a,
          tr(filter).asInstanceOf[Arr], tr(cols).asInstanceOf[List[Col]], tr(vals))
      case Delete(t, a, filter) => Delete(tr(t).asInstanceOf[Ident], a, tr(filter).asInstanceOf[Arr])
      case Arr(els) => Arr(els map tr)
      case Filters(f) => Filters(f map (tr(_).asInstanceOf[Arr]))
      case Braces(expr) => Braces(tr(expr))
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    transform_traverse
  }

  def extractor[T](
    fun: Extractor[T],
    traverser: Extractor[T] = PartialFunction.empty): Extractor[T] = {
    val wrapper: Extractor[T] = fun orElse { case (x, _) => x }
    extractorAndTraverser({ case t => (wrapper(t), true)}, traverser)
  }

  def extractorAndTraverser[T](
    fun: ExtractorAndTraverser[T],
    traverser: Extractor[T] = PartialFunction.empty): Extractor[T] = {
    val noExtr: ExtractorAndTraverser[T] = { case x => (x._1, true) }
    val extr = fun orElse noExtr
    val wrapper: PartialFunction[(T, Exp), (T, Exp)] = {
      case t => extr(t) match {
        case (res, true) => (res, t._2)
        case (res, false) => (res, null)
      }
    }
    def tr(r: T, x: Any): T = x match {
      case e: Exp @unchecked => extract_traverse((r, e))
      case l: List[_] => l.foldLeft(r) { (fr, el) => tr(fr, el) }
      case _ => r
    }
    def traverse_rest(r: T, e: Exp) = e match {
      case _: Ident | _: Id | _: IdRef | _: Res | All | _: IdentAll | _: Variable | Null | null => r
      case Fun(_, pars, _) => tr(r, pars)
      case UnOp(_, operand) => tr(r, operand)
      case BinOp(_, lop, rop) => tr(tr(r, lop), rop)
      case In(lop, rop, _) => tr(tr(r, lop), rop)
      case TerOp(lop, op1, mop, op2, rop) => tr(tr(tr(r, lop), mop), rop)
      case Obj(t, _, j, _, _) => tr(tr(r, j), t) //call tr method in order of writing tresql statement
      case Join(_, j, _) => tr(r, j)
      case Col(c, _, _) => tr(r, c)
      case Cols(_, cols) => tr(r, cols)
      case Grp(cols, hv) => tr(tr(r, cols), hv)
      case Ord(cols) => tr(r, cols.map(_._2))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        tr(tr(tr(tr(tr(tr(tr(r, objs), filters), cols), gr), ord), off), lim)
      case WithTable(_, _, _, q) => tr(r, q)
      case With(ts, q) => tr(tr(r, ts), q)
      case Insert(_, _, cols, vals) => tr(tr(r, cols), vals)
      case Update(_, _, filter, cols, vals) => tr(tr(tr(r, filter), cols), vals)
      case Delete(_, _, filter) => tr(r, filter)
      case Arr(els) => tr(r, els)
      case Filters(f) => tr(r, f)
      case Braces(expr) => tr(r, expr)
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    lazy val extract_traverse: Extractor[T] = wrapper andThen (traverser orElse {
      case (r: T @unchecked, e) => traverse_rest(r, e)
      case (null, e) => traverse_rest(null.asInstanceOf[T], e) //previous case does not match null!
    })
    extract_traverse
  }

  //extract variables in reverse order
  def variableExtractor: Extractor[List[Variable]] = {
    var bindIdx = 0
    ({
      case (l, v @ Variable("?", _, _, _)) =>
        bindIdx += 1
        (v copy bindIdx.toString) :: l
      case (l, v: Variable) => v :: l
    })
  }
}
