package org.tresql.compiling

import org.tresql.parsing._
import org.tresql.metadata._
import org.tresql.Env
import scala.reflect.ManifestFactory

trait Scope {
  def parent: Scope
  def table(table: String): Option[Table]
  def column(col: String): Option[Col[_]]
  def procedure(procedure: String): Option[Procedure[_]]
}

trait Compiler extends QueryParsers with ExpTransformer with Scope { thisCompiler =>

  var nameIdx = 0
  val metadata = Env.metaData

  trait TypedExp[T] extends Exp {
    def exp: Exp
    def typ: Manifest[T]
    def tresql = exp.tresql
  }
  //helper class for namer to distinguish table references from column references
  case class TableObj(obj: Exp) extends Exp {
    def tresql = obj.tresql
  }
  case class ColDef[T](name: String, col: Any, typ: Manifest[T]) extends TypedExp[T] {
    def exp = this
    override def tresql = any2tresql(col)
  }
  case class ChildDef(exp: Exp) extends TypedExp[ChildDef] {
    val typ: Manifest[ChildDef] = ManifestFactory.classType(this.getClass)
  }
  case class FunDef[T](name: String, exp: Fun)(implicit val typ: Manifest[T]) extends TypedExp[T]
  case class TableDef(name: String, exp: Obj) extends Exp { def tresql = exp.tresql }

  //is superclass of sql query and array
  trait RowDefBase extends TypedExp[RowDefBase] {
    def cols: List[ColDef[_]]
    val typ: Manifest[RowDefBase] = ManifestFactory.classType(this.getClass)
  }

  //superclass of select and dml statements (insert, update, delete)
  trait SQLDefBase extends RowDefBase with Scope {
    def tables: List[TableDef]

    protected def this_table(table: String) = tables.find(_.name == table).flatMap {
      case TableDef(_, Obj(TableObj(Ident(name)), _, _, _, _)) => parent.table(name mkString ".")
      case TableDef(n, Obj(TableObj(s: SelectDefBase), _, _, _, _)) => Option(table_from_selectdef(n, s))
    }
    protected def declared_table(table: String): Option[Table] =
      this_table(table) orElse (parent match {
        case p: SelectDef => p.declared_table(table)
        case _ => None
      })
    def table(table: String) = this_table(table) orElse parent.table(table)
    def column(col: String) = col.lastIndexOf('.') match {
      case -1 => tables.collect {
        case TableDef(t, _) => declared_table(t).flatMap(_.colOption(col))
      } collect { case Some(col) => col } match {
        case List(col) => Some(col)
        case Nil => None
        case x => sys.error(s"Ambiguous columns: $x")
      }
      case x =>
        declared_table(col.substring(0, x)).flatMap(_.colOption(col.substring(x + 1)))
    }

    def procedure(procedure: String) = parent.procedure(procedure)

    private def table_from_selectdef(name: String, sd: SelectDefBase) =
      Table(name, sd.cols map col_from_coldef, null, Map())
    private def col_from_coldef(cd: ColDef[_]) =
      org.tresql.metadata.Col(name = cd.name, true, -1, scalaType = cd.typ)
  }

  //is superclass of insert, update, delete
  trait DMLDefBase extends SQLDefBase {
    override def exp: DMLExp
    override def parent = thisCompiler
  }

  //is superclass of select, union, intersect etc.
  trait SelectDefBase extends SQLDefBase

  case class SelectDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: Query,
    parent: Scope) extends SelectDefBase {

    //check for duplicating tables
    {
      val duplicates = tables.groupBy(_.name).filter(_._2.size > 1).map(_._1)
      assert(duplicates.size == 0, s"Duplicate table names: ${duplicates.mkString(", ")}")
    }
  }

  //union, intersect, except ...
  case class BinSelectDef(
    leftOperand: SelectDefBase,
    rightOperand: SelectDefBase,
    exp: BinOp) extends SelectDefBase {

    assert (leftOperand.cols.exists {
        case ColDef(_, All | _: IdentAll, _) => true
        case _ => false
      } || rightOperand.cols.exists {
        case ColDef(_, All | _: IdentAll, _) => true
        case _ => false
      } || leftOperand.cols.size == rightOperand.cols.size,
      s"Column count do not match ${leftOperand.cols.size} != ${rightOperand.cols.size}")
    val cols = leftOperand.cols
    val tables = leftOperand.tables
    val parent = leftOperand.parent
  }

  case class InsertDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: Insert
  ) extends DMLDefBase

  case class UpdateDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: Update
  ) extends DMLDefBase

  case class DeleteDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: Delete
  ) extends DMLDefBase

  case class ArrayDef(cols: List[ColDef[_]]) extends RowDefBase {
    def exp = this
    override def tresql = cols.map(c => any2tresql(c.col)).mkString("[", ", ", "]")
  }

  //root scope implementation
  def table(table: String) = metadata.tableOption(table)
  def column(col: String) = metadata.colOption(col)
  def procedure(procedure: String) = metadata.procedureOption(procedure)
  def parent = null

  //expression def build
  def buildTypedDef(exp: Exp) = {
    trait Ctx
    object QueryCtx extends Ctx //root context
    object TablesCtx extends Ctx //from clause
    object ColsCtx extends Ctx //column clause
    object BodyCtx extends Ctx //where, group by, having, order, limit clauses
    val ctx = scala.collection.mutable.Stack[Ctx](QueryCtx)

    def tr(x: Any): Any = x match {case e: Exp @unchecked => builder(e) case _ => x} //helper function
    lazy val builder: PartialFunction[Exp, Exp] = transformer {
      case f: Fun => procedure(f.name).map { p =>
        FunDef(p.name, f.copy(parameters = f.parameters map tr))(p.scalaReturnType)
      }.getOrElse(sys.error(s"Unknown function: ${f.name}"))
      case c: Col =>
        val alias = if (c.alias != null) c.alias else c.col match {
          case Obj(Ident(name), _, _, _, _) => name.last //use last part of qualified ident as name
          case _ => null
        }
        ColDef(
          alias,
          tr(c.col) match { case x if ctx.head == ColsCtx => x case x: Exp => ChildDef(x) case x => sys.error(s"Exp $x unsupported at this place")},
          if(c.typ != null) metadata.xsd_scala_type_map(c.typ) else ManifestFactory.Nothing
        )
      case Obj(b: Braces, _, _, _, _) if ctx.head == QueryCtx =>
        builder(b) //unwrap braces top level expression
      case o: Obj if ctx.head == QueryCtx | ctx.head == TablesCtx => //obj as query
        builder(Query(List(o), null, null, null, null, null, null))
      case o: Obj if ctx.head == BodyCtx =>
        o.copy(obj = builder(o.obj), join = builder(o.join).asInstanceOf[Join])
      case q: Query =>
        ctx push TablesCtx
        val tables = q.tables map { table =>
          val newTable = builder(table.obj)
          ctx push BodyCtx
          val join = tr(table.join).asInstanceOf[Join]
          ctx.pop
          val name = Option(table.alias).getOrElse(table match {
            case Obj(Ident(name), _, _, _, _) => name mkString "."
            case _ => sys.error(s"Alias missing for from clause select: ${table.tresql}")
          })
          TableDef(name, table.copy(obj = TableObj(newTable), join = join))
        }
        ctx.pop
        ctx push ColsCtx
        val cols =
          if (q.cols != null) (q.cols.cols map {
            case c @ Col(_: DMLExp, _, _) => //child dml statement in select
              ctx push QueryCtx
              val nc = builder(c)
              ctx.pop
              nc
            case c => builder(c)
          }).asInstanceOf[List[ColDef[_]]] match {
            case l if l.exists(_.name == null) => //set names of columns
              l.zipWithIndex.map { case (c, i) =>
                if (c.name == null) c.copy(name = s"_${i + 1}") else c
              }
            case l => l
          }
          else List[ColDef[_]](ColDef[Nothing](null, All, ManifestFactory.Nothing))
        ctx.pop
        ctx push BodyCtx
        val (filter, grp, ord, limit, offset) =
          (tr(q.filter).asInstanceOf[Filters],
           tr(q.group).asInstanceOf[Grp],
           tr(q.order).asInstanceOf[Ord],
           tr(q.limit),
           tr(q.offset))
        ctx.pop
        SelectDef(
          cols,
          tables,
          Query(
            tables = Nil,
            filter = filter,
            cols = null,
            group = grp,
            order = ord,
            limit = limit,
            offset = offset),
          null)
      case b: BinOp =>
        (tr(b.lop), tr(b.rop)) match {
          case (lop: SelectDefBase, rop: SelectDefBase) =>
            BinSelectDef(lop, rop, b.copy(lop = lop, rop = rop))
          case (lop, rop) => b.copy(lop = lop, rop = rop)
        }
      case UnOp("|", o: Exp @unchecked) if ctx.head == ColsCtx =>
        ctx push QueryCtx
        val exp = builder(o)
        ctx.pop
        ChildDef(exp)
      case Braces(exp: Exp) if ctx.head == TablesCtx => builder(exp) //remove braces around table expression, so it can be accessed directly
      case a: Arr if ctx.head == QueryCtx => ArrayDef(
        a.elements.zipWithIndex.map { case (el, idx) =>
          ColDef[Nothing](
            s"_${idx + 1}",
            tr(el) match {
              case s: SQLDefBase => ChildDef(s)
              case a: ArrayDef => ChildDef(a)
              case e => e
            },
            ManifestFactory.Nothing)
        }
      )
      case i: Insert =>
        val table = TableDef(i.alias, Obj(TableObj(i.table), null, null, null))
        ctx push ColsCtx
        val cols =
          if (i.cols != null) i.cols.map {
            case c @ Col(Obj(_: Ident, _, _, _, _), _, _) => builder(c) //insertable col
            case c => //child expression
              ctx push QueryCtx
              val exp = builder(c)
              ctx.pop
              exp
          }.asInstanceOf[List[ColDef[_]]]
          else Nil
        ctx.pop
        ctx push BodyCtx
        val vals = tr(i.vals)
        ctx.pop
        InsertDef(cols, List(table), i.copy(table = null, cols = null, vals = vals))
      case null => null
    }
    builder(exp)
  }

  def resolveScopes(exp: Exp) = {
    val scope_stack = scala.collection.mutable.Stack[Scope](thisCompiler)
    lazy val scoper: PartialFunction[Exp, Exp] = transformer {
      case sd: SelectDef =>
        val nsd = sd.copy(parent = scope_stack.head)
        val t = (nsd.tables map scoper).asInstanceOf[List[TableDef]]
        scope_stack push nsd
        val c = (nsd.cols map scoper).asInstanceOf[List[ColDef[_]]]
        val q = scoper(nsd.exp).asInstanceOf[Query]
        scope_stack.pop
        nsd.copy(cols = c, tables = t, exp = q)
    }
    scoper(exp)
  }

  def resolveColAsterisks(exp: Exp) = {
    def createCol(col: String): Col = try {
      intermediateResults.get.clear
      column(new scala.util.parsing.input.CharSequenceReader(col)).get
    } finally intermediateResults.get.clear

    lazy val resolver: PartialFunction[Exp, Exp] = transformer {
      case sd: SelectDef =>
        val nsd = sd.copy(tables = {
          sd.tables.map {
            case td @ TableDef(_, Obj(TableObj(_: SelectDefBase), _, _, _, _)) =>
              resolver(td).asInstanceOf[TableDef]
            case td => td
          }
        })
        nsd.copy (cols = {
          nsd.cols.flatMap {
            case ColDef(_, All, _) =>
              nsd.tables.flatMap { td =>
                val table = nsd.table(td.name).getOrElse(sys.error(s"Cannot find table: $td"))
                table.cols.map { c => ColDef(c.name, createCol(c.name).col, c.scalaType) }
              }
            case ColDef(_, IdentAll(Ident(ident)), _) =>
              nsd.table(ident mkString ".")
                .map(_.cols.map { c => ColDef(c.name, createCol(c.name).col, c.scalaType) })
                .getOrElse(sys.error(s"Cannot find table: ${ident mkString "."}"))
            case cd @ ColDef(_, chd: ChildDef, _) =>
              List(cd.copy(col = resolver(chd)))
            case cd => List(cd)
          }
        })
    }
    resolver(exp)
  }

  def resolveNames(exp: Exp) = {
    trait Ctx
    object TableCtx extends Ctx
    object ColumnCtx extends Ctx
    case class Context(scope: Scope, ctx: Ctx)
    lazy val namer: PartialFunction[(Context, Exp), Context] = extractorAndTraverser {
      case (ctx, sd: SelectDef) =>
        val nctx = ctx.copy(scope = sd) //create context with this select as a scope
        sd.tables foreach { t =>
          namer(ctx -> t.exp.obj) //table definition check goes within parent scope
          Option(t.exp.join).map(j => namer(nctx -> j)) //join definition check goes within this select scope
        }
        sd.cols foreach (c => namer(nctx -> c))
        namer(nctx -> sd.exp)
        (ctx, false) //return old scope and stop traversing
      case (ctx, id: InsertDef) =>
        id.tables foreach (t => namer(ctx -> t.exp.obj))
        val nctx = ctx.copy(scope = id)
        id.cols foreach (c => namer(nctx -> c))
        namer(ctx -> id.exp)
        (ctx, false) //return old scope and stop traversing
      case (ctx, _: TableObj) => (ctx.copy(ctx = TableCtx), true) //set table context
      case (ctx, _: Obj) => (ctx.copy(ctx = ColumnCtx), true) //set column context
      case (ctx @ Context(scope, TableCtx), Ident(ident)) => //check table
        val tn = ident mkString "."
        scope.table(tn).orElse(sys.error(s"Unknown table: $tn"))
        (ctx, true)
      case (ctx @ Context(scope, ColumnCtx), Ident(ident)) => //check column
        val cn = ident mkString "."
        scope.column(cn).orElse(sys.error(s"Unknown column: $cn"))
        (ctx, true)
    }
    namer(Context(thisCompiler, ColumnCtx) -> exp)
    exp
  }

  def resolveColTypes(exp: Exp) = {
    val scopes = scala.collection.mutable.Stack[Scope](thisCompiler)
    def type_from_any(exp: Any) = exp match {
      case n: java.lang.Number => ManifestFactory.classType(n.getClass)
      case b: Boolean => ManifestFactory.Boolean
      case s: String => ManifestFactory.classType(s.getClass)
      case e: Exp => typer((ManifestFactory.Nothing, e)) /*null cannot be used since partial function does not match it as type T - Manifest*/
      case x => ManifestFactory.classType(x.getClass)
    }
    lazy val typer: PartialFunction[(Manifest[_], Exp), Manifest[_]] = extractorAndTraverser {
      case (_, UnOp(op, operand)) => (type_from_any(operand), false)
      case (_, BinOp(op, lop, rop)) =>
        comp_op.findAllIn(op).toList match {
          case Nil =>
            val (lt, rt) = (type_from_any(lop), type_from_any(rop))
            (if (lt.toString == "java.lang.String") lt else if (rt == "java.lang.String") rt
            else if (lt.toString == "java.lang.Boolean") lt else if (rt == "java.lang.Boolean") rt
            else if (lt <:< rt) rt else if (rt <:< lt) lt else lt, false)
          case _ => (Manifest.Boolean, false)
        }
      case (_, _: TerOp) => (Manifest.Boolean, false)
      case (_, s: SelectDef) =>
        if (s.cols.size > 1)
          sys.error(s"Select must contain only one column, instead:${s.cols.map(_.tresql).mkString(", ")}")
        else {
          scopes.push(s)
          val ret = (type_from_any(s.cols.head), false)
          scopes.pop
          ret
        }
      case (_, Ident(ident)) => (scopes.head.column(ident mkString ".").map(_.scalaType).get, false)
    }
    lazy val type_resolver: PartialFunction[Exp, Exp] = transformer {
      case s: SelectDef =>
        //resolve column types for potential from clause select definitions
        val nsd = s.copy(tables = (s.tables map type_resolver).asInstanceOf[List[TableDef]])
        scopes.push(nsd)
        val ncols = (nsd.cols map type_resolver).asInstanceOf[List[ColDef[_]]] //resolve types for column defs
        scopes.pop
        nsd.copy(cols = ncols)
      case i: InsertDef =>
        scopes.push(i)
        val ncols = (i.cols map type_resolver).asInstanceOf[List[ColDef[_]]]
        scopes.pop
        i.copy(cols = ncols)
      case ColDef(n, ChildDef(ch), t) => ColDef(n, ChildDef(type_resolver(ch)), t)
      case ColDef(n, exp, typ) if typ == null || typ == Manifest.Nothing =>
        ColDef(n, exp, type_from_any(exp))
    }
    type_resolver(exp)
  }

  def compile(exp: Exp) = {
    resolveColTypes(
      resolveNames(
        resolveColAsterisks(
          resolveScopes(
            buildTypedDef(
              exp)))))
  }

  override def transformer(fun: PartialFunction[Exp, Exp]): PartialFunction[Exp, Exp] = {
    lazy val local_transformer = fun orElse traverse
    lazy val transform_traverse = local_transformer orElse super.transformer(local_transformer)
    def tr(x: Any): Any = x match {case e: Exp @unchecked => transform_traverse(e) case _ => x} //helper function
    lazy val traverse: PartialFunction[Exp, Exp] = {
      case cd: ColDef[_] => cd.copy(col = tr(cd.col))
      case cd: ChildDef => cd.copy(exp = transform_traverse(cd.exp))
      case fd: FunDef[_] => fd.copy(exp = transform_traverse(fd.exp).asInstanceOf[Fun])
      case td: TableDef => td.copy(exp = transform_traverse(td.exp).asInstanceOf[Obj])
      case to: TableObj => to.copy(obj = transform_traverse(to.obj))
      case sd: SelectDef =>
        val t = (sd.tables map transform_traverse).asInstanceOf[List[TableDef]]
        val c = (sd.cols map transform_traverse).asInstanceOf[List[ColDef[_]]]
        val q = transform_traverse(sd.exp).asInstanceOf[Query]
        sd.copy(cols = c, tables = t, exp = q)
      case bd: BinSelectDef => bd.copy(
        leftOperand = transform_traverse(bd.leftOperand).asInstanceOf[SelectDefBase],
        rightOperand = transform_traverse(bd.rightOperand).asInstanceOf[SelectDefBase])
      case id: InsertDef =>
        val t = (id.tables map transform_traverse).asInstanceOf[List[TableDef]]
        val c = (id.cols map transform_traverse).asInstanceOf[List[ColDef[_]]]
        val i = transform_traverse(id.exp).asInstanceOf[Insert]
        InsertDef(c, t, i)
      case ud: UpdateDef =>
        val t = (ud.tables map transform_traverse).asInstanceOf[List[TableDef]]
        val c = (ud.cols map transform_traverse).asInstanceOf[List[ColDef[_]]]
        val u = transform_traverse(ud.exp).asInstanceOf[Update]
        UpdateDef(c, t, u)
      case dd: DeleteDef =>
        val t = (dd.tables map transform_traverse).asInstanceOf[List[TableDef]]
        val c = (dd.cols map transform_traverse).asInstanceOf[List[ColDef[_]]]
        val d = transform_traverse(dd.exp).asInstanceOf[Delete]
        DeleteDef(c, t, d)
      case ad: ArrayDef => ad.copy(cols = (ad.cols map transform_traverse).asInstanceOf[List[ColDef[_]]])
    }
    transform_traverse
  }

  override def extractorAndTraverser[T](
    fun: PartialFunction[(T, Exp), (T, Boolean)],
    traverser: PartialFunction[(T, Exp), T] = PartialFunction.empty):
  PartialFunction[(T, Exp), T] = {
    def tr(r: T, x: Any): T = x match {
      case e: Exp => extract_traverse((r, e))
      case l: List[_] => l.foldLeft(r) { (fr, el) => tr(fr, el) }
      case _ => r
    }
    lazy val extract_traverse: PartialFunction[(T, Exp), T] =
      super.extractorAndTraverser(fun, traverser orElse local_extract_traverse)
    lazy val local_extract_traverse: PartialFunction[(T, Exp), T] = {
      case (r: T, cd: ColDef[_]) => tr(r, cd.col)
      case (r: T, cd: ChildDef) => tr(r, cd.exp)
      case (r: T, fd: FunDef[_]) => tr(r, fd.exp)
      case (r: T, td: TableDef) => tr(r, td.exp)
      case (r: T, to: TableObj) => tr(r, to.obj)
      case (r: T, sd: SelectDef) => tr(tr(tr(r, sd.tables), sd.cols), sd.exp)
      case (r: T, bd: BinSelectDef) => tr(tr(r, bd.leftOperand), bd.rightOperand)
      case (r: T, id: InsertDef) => tr(tr(tr(r, id.tables), id.cols), id.exp)
      case (r: T, ud: UpdateDef) => tr(tr(tr(r, ud.tables), ud.cols), ud.exp)
      case (r: T, dd: DeleteDef) => tr(tr(tr(r, dd.tables), dd.cols), dd.exp)
      case (r: T, ad: ArrayDef) => tr(r, ad.cols)
    }
    extract_traverse
  }

  def parseExp(expr: String): Any = try {
    intermediateResults.get.clear
    phrase(exprList)(new scala.util.parsing.input.CharSequenceReader(expr)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }
  } finally intermediateResults.get.clear
}
