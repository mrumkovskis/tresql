package org.tresql

trait Transformer { self: QueryBuilder =>

  def transform(expr: Expr, f: PartialFunction[Expr, Expr]): Expr = {
    var cf: PartialFunction[Expr, Expr] = null
    cf = f.orElse[Expr, Expr] {
      case null => null
      case e if e.builder != self => e.builder.transform(e, f)
    }.orElse[Expr, Expr] {
      case ArrExpr(e) => ArrExpr(e map cf)
      case BinExpr(o, lop, rop) => BinExpr(o, cf(lop), cf(rop))
      case BracesExpr(b) => BracesExpr(cf(b))
      case ColExpr(col, alias, sepQuery, hidden) => ColExpr(cf(col), alias, sepQuery, hidden)
      case cols: ColsExpr => cols.copy(cols = (cols.cols map cf).asInstanceOf[List[ColExpr]])
      case FunExpr(n, p, d, o, f) => FunExpr(n, p map cf, d, o map cf, f map cf)
      case FunAsTableExpr(e, cd, ord) => FunAsTableExpr(cf(e), cd, ord)
      case Group(e, h) => Group(e map cf, cf(h))
      case HiddenColRefExpr(e, typ) => HiddenColRefExpr(cf(e), typ)
      case InExpr(lop, rop, not) => InExpr(cf(lop), rop map cf, not)
      case wi: WithInsertExpr => new WithInsertExpr(
        wi.tables.map(cf(_).asInstanceOf[WithTableExpr]), cf(wi.query).asInstanceOf[InsertExpr])
      case i: InsertExpr => new InsertExpr(cf(i.table).asInstanceOf[IdentExpr], i.alias,
        i.cols map cf, cf(i.vals), (i.returning map cf).asInstanceOf[Option[ColsExpr]])
      case Order(exprs) => Order(exprs map (e => (cf(e._1), cf(e._2), cf(e._3))))
      case SelectExpr(tables, filter, cols, distinct, group, order,
        offset, limit, aliases, parentJoin) =>
        SelectExpr(tables map (t => cf(t).asInstanceOf[Table]), cf(filter),
          cf(cols).asInstanceOf[ColsExpr], distinct, cf(group), cf(order),
          cf(offset), cf(limit), aliases, parentJoin map cf)
      case Table(texpr, alias, join, outerJoin, nullable) =>
        Table(cf(texpr), alias, cf(join).asInstanceOf[TableJoin], outerJoin, nullable)
      case TableJoin(default, expr, noJoin, defaultJoinCols) =>
        TableJoin(default, cf(expr), noJoin, defaultJoinCols)
      case WithTableExpr(n, c, r, q) => WithTableExpr(n, c, r, cf(q))
      case WithSelectExpr(tables, query) =>
        WithSelectExpr(tables.map(cf(_).asInstanceOf[WithTableExpr]), cf(query).asInstanceOf[SelectExpr])
      case WithBinExpr(tables, query) =>
        WithBinExpr(tables.map(cf(_).asInstanceOf[WithTableExpr]), cf(query).asInstanceOf[BinExpr])
      case UnExpr(o, op) => UnExpr(o, cf(op))
      case CastExpr(e, t) => CastExpr(cf(e), t)
      case u: UpdateExpr => new UpdateExpr(cf(u.table).asInstanceOf[IdentExpr], u.alias,
        u.filter map cf, u.cols map cf, cf(u.vals), (u.returning map cf).asInstanceOf[Option[ColsExpr]])
      case d: DeleteExpr => //put delete at the end since it is superclass of insert and update
        DeleteExpr(cf(d.table).asInstanceOf[IdentExpr], d.alias, d.filter map cf, cf(d.using),
          (d.returning map cf).asInstanceOf[Option[ColsExpr]])
      case ValuesExpr(vals) => ValuesExpr(vals map cf)
      case vfs: ValuesFromSelectExpr => vfs.copy(select = cf(vfs.select).asInstanceOf[SelectExpr])
      case e => e
    }
    cf(expr)
  }
}
