package org.tresql.parsing

trait ExpTransformer { this: QueryParsers =>

  type Transformer = PartialFunction[Exp, Exp]
  type TransformerWithState[T] = T => PartialFunction[Exp, Exp]
  type Extractor[T] = PartialFunction[(T, Exp), T]
  type ExtractorAndTraverser[T] = PartialFunction[(T, Exp), (T, Boolean)]
  type Traverser[T] = T => PartialFunction[Exp, T]

  def transformer(fun: Transformer): Transformer = {
    lazy val transform_traverse = fun orElse traverse
    //helper function
    def tt[A <: Exp](exp: A) = transform_traverse(exp).asInstanceOf[A]
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
      case Obj(t, a, j, o, n) => Obj(tt(t), a, tt(j), o, n)
      case Join(d, j, n) => Join(d, tt(j), n)
      case Col(c, a, t) => Col(tt(c), a, t)
      case Cols(d, cols) => Cols(d, (cols map tt))
      case Grp(cols, hv) => Grp(cols map tt, tt(hv))
      case Ord(cols) => Ord(cols map (c=> (c._1, tt(c._2), c._3)))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        Query((objs map tt), tt(filters), tt(cols), tt(gr), tt(ord), tt(off), tt(lim))
      case WithTable(n, c, r, q) => WithTable(n, c, r, tt(q))
      case With(ts, q) => With((ts map tt), tt(q))
      case Insert(t, a, cols, vals) => Insert(tt(t), a, (cols map tt), tt(vals))
      case Update(t, a, filter, cols, vals) => Update(tt(t), a, tt(filter), (cols map tt), tt(vals))
      case Delete(t, a, filter) => Delete(tt(t), a, tt(filter))
      case Arr(els) => Arr(els map tt)
      case Filters(f) => Filters((f map tt))
      case Braces(expr) => Braces(tt(expr))
      case null => null
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    transform_traverse
  }

  def transformerWithState[T](fun: TransformerWithState[T]): TransformerWithState[T] = {
    def transform_traverse(state: T): Transformer = fun(state) orElse traverse(state)
    //helper function
    def tt[A <: Exp](state: T)(exp: A) = transform_traverse(state)(exp).asInstanceOf[A]
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
      case Obj(t, a, j, o, n) => Obj(tt(state)(t), a, tt(state)(j), o, n)
      case Join(d, j, n) => Join(d, tt(state)(j), n)
      case Col(c, a, t) => Col(tt(state)(c), a, t)
      case Cols(d, cols) => Cols(d, cols map (tt(state)(_)))
      case Grp(cols, hv) => Grp(cols.map(tt(state)(_)), tt(state)(hv))
      case Ord(cols) => Ord(cols map (c=> (c._1, tt(state)(c._2), c._3)))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        Query(
          objs map (tt(state)(_)),
          tt(state)(filters),
          tt(state)(cols),
          tt(state)(gr),
          tt(state)(ord),
          tt(state)(off),
          tt(state)(lim)
        )
      case WithTable(n, c, r, q) => WithTable(n, c, r, tt(state)(q))
      case With(ts, q) => With((ts map {wt => tt(state)(wt) }), tt(state)(q))
      case Insert(t, a, cols, vals) => Insert(tt(state)(t), a,
          (cols map { c => tt(state)(c) }), tt(state)(vals))
      case Update(table, alias, filter, cols, vals) =>
        Update(
          tt(state)(table),
          alias,
          tt(state)(filter),
          (cols map { c => tt(state)(c) }),
          tt(state)(vals)
        )
      case Delete(table, alias, filter) => Delete(tt(state)(table), alias, tt(state)(filter))
      case Arr(els) => Arr(els.map(tt(state)(_)))
      case Filters(f) => Filters(f map (tt(state)(_)))
      case Values(v) => Values(v map (tt(state)(_)))
      case Braces(expr) => Braces(tt(state)(expr))
      case null => null
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    transform_traverse _
  }

  def traverser[T](fun: Traverser[T]): Traverser[T] = {
    def fun_traverse(state: T) = fun(state) orElse traverse(state)
    def tr(r: T, e: Exp): T = fun_traverse(r)(e)
    def trl(r: T, l: List[Exp]) = l.foldLeft(r) { (fr, el) => tr(fr, el) }
    def traverse(state: T): PartialFunction[Exp, T] = {
      case _: Ident | _: Id | _: IdRef | _: Res | All | _: IdentAll | _: Variable | Null | _: Const | null => state
      case Fun(_, pars, _) => trl(state, pars)
      case UnOp(_, operand) => tr(state, operand)
      case BinOp(_, lop, rop) => tr(tr(state, lop), rop)
      case In(lop, rop, _) => trl(tr(state, lop), rop)
      case TerOp(lop, op1, mop, op2, rop) => tr(tr(tr(state, lop), mop), rop)
      case Obj(t, _, j, _, _) => tr(tr(state, j), t) //call tr method in order of writing tresql statement
      case Join(_, j, _) => tr(state, j)
      case Col(c, _, _) => tr(state, c)
      case Cols(_, cols) => trl(state, cols)
      case Grp(cols, hv) => tr(trl(state, cols), hv)
      case Ord(cols) => trl(state, cols.map(_._2))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        tr(tr(tr(tr(tr(tr(trl(state, objs), filters), cols), gr), ord), off), lim)
      case WithTable(_, _, _, q) => tr(state, q)
      case With(ts, q) => tr(trl(state, ts), q)
      case Insert(_, _, cols, vals) => tr(trl(state, cols), vals)
      case Update(_, _, filter, cols, vals) => tr(trl(tr(state, filter), cols), vals)
      case Delete(_, _, filter) => tr(state, filter)
      case Arr(els) => trl(state, els)
      case Filters(f) => trl(state, f)
      case Values(v) => trl(state, v)
      case Braces(expr) => tr(state, expr)
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    fun_traverse _
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
