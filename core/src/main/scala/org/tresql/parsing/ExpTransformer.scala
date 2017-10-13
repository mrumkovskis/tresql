package org.tresql.parsing

trait ExpTransformer { this: QueryParsers =>

  type Transformer = PartialFunction[Exp, Exp]
  type TransformerWithState[T] = T => PartialFunction[Exp, Exp]
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
      case Fun(n, pars, d, o, f) => Fun(n, pars map tt, d, o map tt, f map tt)
      case Cast(e, t) => Cast(tt(e), t)
      case UnOp(o, op) => UnOp(o, tt(op))
      case BinOp(o, lop, rop) => BinOp(o, tt(lop), tt(rop))
      case TerOp(lop, op1, mop, op2, rop) => TerOp(tt(lop), op1, tt(mop), op2, tt(rop))
      case In(lop, rop, not) => In(tt(lop), rop map tt, not)
      case Obj(t, a, j, o, n) => Obj(tt(t), a, tt(j), o, n)
      case Join(d, j, n) => Join(d, tt(j), n)
      case Col(c, a) => Col(tt(c), a)
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
      case Values(v) => Values(v map tt)
      case ValuesFromSelect(s, a) => ValuesFromSelect(tt(s), a)
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
      case Fun(n, pars, d, o, f) =>
        Fun(n, pars map tt(state), d, o map tt(state), f map tt(state))
      case Cast(e, t) => Cast(tt(state)(e), t)
      case UnOp(o, op) => UnOp(o, tt(state)(op))
      case BinOp(o, lop, rop) => BinOp(o, tt(state)(lop), tt(state)(rop))
      case TerOp(lop, op1, mop, op2, rop) => TerOp(tt(state)(lop), op1, tt(state)(mop), op2, tt(state)(rop))
      case In(lop, rop, not) => In(tt(state)(lop), rop map tt(state), not)
      case Obj(t, a, j, o, n) => Obj(tt(state)(t), a, tt(state)(j), o, n)
      case Join(d, j, n) => Join(d, tt(state)(j), n)
      case Col(c, a) => Col(tt(state)(c), a)
      case Cols(d, cols) => Cols(d, cols map tt(state))
      case Grp(cols, hv) => Grp(cols map tt(state), tt(state)(hv))
      case Ord(cols) => Ord(cols map (c=> (c._1, tt(state)(c._2), c._3)))
      case Query(objs, filters, cols, gr, ord, off, lim) =>
        Query(
          objs map tt(state),
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
      case Arr(els) => Arr(els map tt(state))
      case Filters(f) => Filters(f map tt(state))
      case Values(v) => Values(v map tt(state))
      case ValuesFromSelect(s, a) => ValuesFromSelect(tt(state)(s), a)
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
      case Fun(_, pars, _, o, f) =>
        val ps = trl(state, pars)
        val os = o.map(tr(ps, _)).getOrElse(ps)
        f.map(tr(os, _)).getOrElse(os)
      case Cast(e, _) => tr(state, e)
      case UnOp(_, operand) => tr(state, operand)
      case BinOp(_, lop, rop) => tr(tr(state, lop), rop)
      case In(lop, rop, _) => trl(tr(state, lop), rop)
      case TerOp(lop, op1, mop, op2, rop) => tr(tr(tr(state, lop), mop), rop)
      case Obj(t, _, j, _, _) => tr(tr(state, j), t) //call tr method in order of writing tresql statement
      case Join(_, j, _) => tr(state, j)
      case Col(c, _) => tr(state, c)
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
      case ValuesFromSelect(s, _) => tr(state, s)
      case Braces(expr) => tr(state, expr)
      //for debugging purposes throw an exception since all expressions must be matched above for complete traversal
      case x: Exp => sys.error("Unknown expression: " + x)
    }
    fun_traverse _
  }

  /** Extract variables in reverse order. Variable names '?' are replaced with index starting with 1 */
  def variableExtractor: Traverser[List[Variable]] = {
    var bindIdx = 0
    vars => {
      case v @ Variable("?", _, _) =>
        bindIdx += 1
        (v copy bindIdx.toString) :: vars
      case v: Variable => v :: vars
    }
  }
}
