package org.tresql.parsing

trait ExpTransformer { this: QueryParsers =>

  type Transformer = PartialFunction[Exp, Exp]
  type TransformerWithState[T] = (T) => PartialFunction[Exp, Exp]
  type Extractor[T] = PartialFunction[(T, Exp), T]
  type ExtractorAndTraverser[T] = PartialFunction[(T, Exp), (T, Boolean)]

  def transformer(fun: Transformer): Transformer = {
    //transform traverse function
    lazy val tt = fun orElse traverse
    lazy val traverse: Transformer = {
      case All => All
      case Null => Null
      case c: Const => c
      case e: Ident => e
      case e: Id => e
      case e: IdRef => e
      case e: Res => e
      case e: IdentAll => e
      case e: Variable => e
      case Fun(n, pars, d) => Fun(n, pars map tt, d)
      case UnOp(o, op) => UnOp(o, tt(op))
      case BinOp(o, lop, rop) => BinOp(o, tt(lop), tt(rop))
      case TerOp(lop, op1, mop, op2, rop) => TerOp(tt(lop), op1, tt(mop), op2, tt(rop))
      case In(lop, rop, not) => In(tt(lop), rop map tt, not)
      case Obj(t, a, j, o, n) => Obj(tt(t), a, tt(j).asInstanceOf[Join], o, n)
      case Join(d, j, n) => Join(d, tt(j), n)
      case Col(c, a, t) => Col(tt(c), a, t)
      case Cols(d, cols) => Cols(d, (cols map tt).asInstanceOf[List[Col]])
      case Grp(cols, hv) => Grp(cols map tt, tt(hv))
      case Ord(cols) => Ord(cols map (c=> (c._1, tt(c._2), c._3)))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        Query((objs map tt).asInstanceOf[List[Obj]], tt(filters).asInstanceOf[Filters],
            tt(cols).asInstanceOf[Cols], tt(gr).asInstanceOf[Grp], tt(ord).asInstanceOf[Ord],
            tt(off), tt(lim))
      case WithTable(n, c, r, q) => WithTable(n, c, r, tt(q))
      case With(ts, q) => With((ts map tt).asInstanceOf[List[WithTable]], tt(q))
      case Insert(t, a, cols, vals) => Insert(tt(t).asInstanceOf[Ident], a,
        (cols map tt).asInstanceOf[List[Col]], tt(vals))
      case Update(t, a, filter, cols, vals) => Update(tt(t).asInstanceOf[Ident], a,
          tt(filter).asInstanceOf[Arr], (cols map tt).asInstanceOf[List[Col]], tt(vals))
      case Delete(t, a, filter) => Delete(tt(t).asInstanceOf[Ident], a, tt(filter).asInstanceOf[Arr])
      case Arr(els) => Arr(els map tt)
      case Filters(f) => Filters((f map tt).asInstanceOf[List[Arr]])
      case Braces(expr) => Braces(tt(expr))
      case null => null
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    tt
  }

  def transformerWithState[T](fun: TransformerWithState[T]): TransformerWithState[T] = {
    //transform traverse
    def tt(state: T): Transformer = fun(state) orElse traverse(state)
    def traverse(state: T): Transformer = {
      case All => All
      case Null => Null
      case c: Const => c
      case e: Ident => e
      case e: Id => e
      case e: IdRef => e
      case e: Res => e
      case e: IdentAll => e
      case e: Variable => e
      case Fun(n, pars, d) => Fun(n, pars.map(tt(state)(_)), d)
      case UnOp(o, op) => UnOp(o, tt(state)(op))
      case BinOp(o, lop, rop) => BinOp(o, tt(state)(lop), tt(state)(rop))
      case TerOp(lop, op1, mop, op2, rop) => TerOp(tt(state)(lop), op1, tt(state)(mop), op2, tt(state)(rop))
      case In(lop, rop, not) => In(tt(state)(lop), rop.map(tt(state)(_)), not)
      case Obj(t, a, j, o, n) => Obj(tt(state)(t), a, tt(state)(j).asInstanceOf[Join], o, n)
      case Join(d, j, n) => Join(d, tt(state)(j), n)
      case Col(c, a, t) => Col(tt(state)(c), a, t)
      case Cols(d, cols) => Cols(d, cols map (tt(state)(_).asInstanceOf[Col]))
      case Grp(cols, hv) => Grp(cols.map(tt(state)(_)), tt(state)(hv))
      case Ord(cols) => Ord(cols map (c=> (c._1, tt(state)(c._2), c._3)))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        Query(
          objs map (tt(state)(_).asInstanceOf[Obj]),
          tt(state)(filters).asInstanceOf[Filters],
          tt(state)(cols).asInstanceOf[Cols],
          tt(state)(gr).asInstanceOf[Grp],
          tt(state)(ord).asInstanceOf[Ord],
          tt(state)(off),
          tt(state)(lim)
        )
      case WithTable(n, c, r, q) => WithTable(n, c, r, tt(state)(q))
      case With(ts, q) => With((ts map {wt => tt(state)(wt) }).asInstanceOf[List[WithTable]],
        tt(state)(q))
      case Insert(t, a, cols, vals) => Insert(tt(state)(t).asInstanceOf[Ident], a,
          (cols map { c => tt(state)(c) }).asInstanceOf[List[Col]], tt(state)(vals))
      case Update(t, a, filter, cols, vals) =>
        Update(
          tt(state)(t).asInstanceOf[Ident],
          a,
          tt(state)(filter).asInstanceOf[Arr],
          (cols map { c => tt(state)(c) }).asInstanceOf[List[Col]],
          tt(state)(vals)
        )
      case Delete(t, a, filter) =>
        Delete(tt(state)(t).asInstanceOf[Ident], a, tt(state)(filter).asInstanceOf[Arr])
      case Arr(els) => Arr(els.map(tt(state)(_)))
      case Filters(f) => Filters(f map (tt(state)(_).asInstanceOf[Arr]))
      case Values(v) => Values(v map (tt(state)(_).asInstanceOf[Arr]))
      case Braces(expr) => Braces(tt(state)(expr))
      case null => null
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    tt _
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
    //shortcut
    def tr(r: T, e: Exp): T = extract_traverse((r, e))
    def trl(r: T, l: List[Exp]) = l.foldLeft(r) { (fr, el) => tr(fr, el) }
    def traverse_rest(r: T, e: Exp) = e match {
      case _: Ident | _: Id | _: IdRef | _: Res | All | _: IdentAll | _: Variable | Null | _: Const | null => r
      case Fun(_, pars, _) => trl(r, pars)
      case UnOp(_, operand) => tr(r, operand)
      case BinOp(_, lop, rop) => tr(tr(r, lop), rop)
      case In(lop, rop, _) => trl(tr(r, lop), rop)
      case TerOp(lop, op1, mop, op2, rop) => tr(tr(tr(r, lop), mop), rop)
      case Obj(t, _, j, _, _) => tr(tr(r, j), t) //call tr method in order of writing tresql statement
      case Join(_, j, _) => tr(r, j)
      case Col(c, _, _) => tr(r, c)
      case Cols(_, cols) => trl(r, cols)
      case Grp(cols, hv) => tr(trl(r, cols), hv)
      case Ord(cols) => trl(r, cols.map(_._2))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        tr(tr(tr(tr(tr(tr(trl(r, objs), filters), cols), gr), ord), off), lim)
      case WithTable(_, _, _, q) => tr(r, q)
      case With(ts, q) => tr(trl(r, ts), q)
      case Insert(_, _, cols, vals) => tr(trl(r, cols), vals)
      case Update(_, _, filter, cols, vals) => tr(trl(tr(r, filter), cols), vals)
      case Delete(_, _, filter) => tr(r, filter)
      case Arr(els) => trl(r, els)
      case Filters(f) => trl(r, f)
      case Values(v) => trl(r, v)
      case Braces(expr) => tr(r, expr)
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    //extract traverse
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
