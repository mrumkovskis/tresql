package org.tresql
package compiling

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

  case class CompilerException(
    message: String,
    pos: scala.util.parsing.input.Position = scala.util.parsing.input.NoPosition
  ) extends Exception(message)

  var nameIdx = 0
  val metadata = Env.metaData

  trait TypedExp[T] extends Exp {
    def exp: Exp
    def typ: Manifest[T]
    def tresql = exp.tresql
  }

  case class TableDef(name: String, exp: Obj) extends Exp { def tresql = exp.tresql }
  //helper class for namer to distinguish table references from column references
  case class TableObj(obj: Exp) extends Exp {
    def tresql = obj.tresql
  }
  //helper class for namer to distinguish table with NoJoin, i.e. must be defined it tables clause earlier
  case class TableAlias(obj: Exp) extends Exp {
    def tresql = obj.tresql
  }
  case class ColDef[T](name: String, col: Any, typ: Manifest[T]) extends TypedExp[T] {
    def exp = this
    override def tresql = any2tresql(col)
  }
  case class ChildDef(exp: Exp) extends TypedExp[ChildDef] {
    val typ: Manifest[ChildDef] = ManifestFactory.classType(this.getClass)
  }
  case class FunDef[T](name: String, exp: Fun, typ: Manifest[T], procedure: Procedure[_])
    extends TypedExp[T] {
    if((procedure.hasRepeatedPar && exp.parameters.size < procedure.pars.size - 1) ||
      (!procedure.hasRepeatedPar && exp.parameters.size != procedure.pars.size))
      throw CompilerException(s"Function '$name' has wrong number of parameters: ${exp.parameters.size}")
  }
  case class RecursiveDef(exp: Exp, scope: Scope = null) extends TypedExp[RecursiveDef]
  with Scope {
    val typ: Manifest[RecursiveDef] = ManifestFactory.classType(this.getClass)
    override def parent = scope.parent
    override def table(table: String) = scope.table(table)
    override def column(col: String) = scope.column(col)
    override def procedure(procedure: String) = scope.procedure(procedure)
  }

  //is superclass of sql query and array
  trait RowDefBase extends TypedExp[RowDefBase] {
    def cols: List[ColDef[_]]
    val typ: Manifest[RowDefBase] = ManifestFactory.classType(this.getClass)
  }

  //superclass of select and dml statements (insert, update, delete)
  trait SQLDefBase extends RowDefBase with Scope {
    def tables: List[TableDef]
    override def parent: Scope = thisCompiler

    protected def this_table(table: String) = tables.find(_.name == table).flatMap {
      case TableDef(_, Obj(TableObj(Ident(name)), _, _, _, _)) => parent.table(name mkString ".")
      case TableDef(n, Obj(TableObj(s: SelectDefBase), _, _, _, _)) => Option(table_from_selectdef(n, s))
      case x => throw CompilerException(s"Unrecognized table clause: '${x.tresql}'. Try using Query(...)")
    }
    protected def declared_table(table: String): Option[Table] =
      this_table(table) orElse (parent match {
        case p: SQLDefBase => p.declared_table(table)
        case _ => None
      })
    def table(tableName: String) = {
      val table = tableName.toLowerCase
      this_table(table) orElse parent.table(table)
    }
    def column(colName: String) = {
      val col = colName.toLowerCase
      col.lastIndexOf('.') match {
        case -1 => tables.collect {
          //collect columns only from tables (not aliases)
          case TableDef(t, Obj(_: TableObj, _, _, _, _)) =>
            declared_table(t).flatMap(_.colOption(col))
        } collect { case Some(col) => col } match {
          case List(col) => Some(col)
          case Nil => None
          case x => throw CompilerException(s"Ambiguous columns: $x")
        }
        case x =>
          declared_table(col.substring(0, x)).flatMap(_.colOption(col.substring(x + 1)))
      }
    } orElse procedure(s"$colName#0").map(p => //check for empty pars function declaration
      org.tresql.metadata.Col(name = colName, true, -1, scalaType = p.scalaReturnType))

    def procedure(procedure: String) = parent.procedure(procedure)

    protected def table_from_selectdef(name: String, sd: SelectDefBase) =
      Table(name, sd.cols map col_from_coldef, null, Map())
    protected def col_from_coldef(cd: ColDef[_]) =
      org.tresql.metadata.Col(name = cd.name, true, -1, scalaType = cd.typ)
  }

  //is superclass of insert, update, delete
  trait DMLDefBase extends SQLDefBase {
    override def exp: DMLExp
  }

  //is superclass of select, union, intersect etc.
  trait SelectDefBase extends SQLDefBase {
    def cols: List[ColDef[_]]
  }

  case class SelectDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: Query,
    override val parent: Scope = thisCompiler) extends SelectDefBase {
    //check for duplicating tables
    {
      val duplicates = tables.filter { //filter out aliases
        case TableDef(_, Obj(_: TableAlias, _, _, _, _)) => false
        case _ => true
      }.groupBy(_.name).filter(_._2.size > 1)
      if(duplicates.size > 0) throw CompilerException(
        s"Duplicate table names: ${duplicates.mkString(", ")}")
    }
  }

  //union, intersect, except ...
  case class BinSelectDef(
    leftOperand: SelectDefBase,
    rightOperand: SelectDefBase,
    exp: BinOp) extends SelectDefBase {

    if (!(leftOperand.cols.exists {
        case ColDef(_, All | _: IdentAll, _) => true
        case _ => false
      } || rightOperand.cols.exists {
        case ColDef(_, All | _: IdentAll, _) => true
        case _ => false
      }) && leftOperand.cols.size != rightOperand.cols.size)
      throw CompilerException(
        s"Column count do not match ${leftOperand.cols.size} != ${rightOperand.cols.size}")
    def cols = leftOperand.cols
    def tables = leftOperand.tables
    override val parent = leftOperand.parent
  }

  case class WithTableDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    recursive: Boolean,
    exp: SQLDefBase,
    override val parent: Scope = thisCompiler
  ) extends SelectDefBase {
    if (recursive) {
      exp match {
        case _: BinSelectDef =>
        case q => throw CompilerException(s"Recursive table definition must be union, instead found: ${q.tresql}")
      }
      if (cols.isEmpty) throw CompilerException(s"Recursive table definition must have at least one column")
    }
    if (!cols.isEmpty && cols.size != exp.cols.size)
      throw CompilerException(s"with table definition column count must equal corresponding query definition column count: ${exp.tresql}")
    override protected[compiling] def this_table(table: String) = tables.find(_.name == table).map {
      case _ => table_from_selectdef(table, this)
    }
  }

  case class WithSelectDef(
    exp: SelectDefBase,
    withTables: List[WithTableDef],
    override val parent: Scope = thisCompiler
  ) extends SelectDefBase {
    def cols = exp.cols
    def tables = exp.tables
    override protected def this_table(table: String) = {
      def t(wts: List[WithTableDef]): Option[Table] = wts match {
        case Nil => parent.table(table)
        case wt :: tail => wt.this_table(table) orElse t(tail)
      }
      t(withTables)
    }
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
    tables: List[TableDef],
    exp: Delete
  ) extends DMLDefBase {
    def cols = Nil
  }

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
    def buildTables(tables: List[Obj]): List[TableDef] = {
      ctx push TablesCtx
      try {
        val td1 = tables.zipWithIndex map { case (table, idx) =>
          val newTable = builder(table.obj)
          ctx push BodyCtx
          val join = tr(table.join).asInstanceOf[Join]
          ctx.pop
          val name = Option(table.alias).getOrElse(table match {
            case Obj(Ident(name), _, _, _, _) => name mkString "."
            case _ => s"_${idx + 1}"
          })
          TableDef(name, table.copy(obj = TableObj(newTable), join = join)) -> idx
        }
        //process no join aliases
        val td2 = td1 map { case (table, idx) => table match {
            //NoJoin alias
            case td @ TableDef(_, o @ Obj(TableObj(Ident(List(alias))), _,
              Join(_, _, true), _, _)) => //check if alias exists
              if (td1.view(0, idx).exists(_._1.name == alias))
                td.copy(exp = o.copy(obj = TableAlias(Ident(List(alias)))))
              else throw CompilerException(s"No join table not defined: $alias")
            case TableDef(_, Obj(x, _, Join(_, _, true), _, _)) =>
              throw CompilerException(s"Unsupported no join table: $x")
            case x => x //ordinary table def
          }
        }
        //add table alias to foreign key alias joins with unqualified names
        td2.tail.scanLeft(td2.head) {
          case (left,
            right @ TableDef(_,
              exp @ Obj(_: TableObj, _,
                join @ Join(_,
                  expr @ Obj(Ident(List(fkAlias)), _, _, _, _), false), _, _))) =>
                //add left table alias to foreign key alias join ident
                right.copy(exp =
                  exp.copy(join =
                    join.copy(expr =
                      expr.copy(obj =
                        Ident(List(left.name, fkAlias))))))
          case (left, right) => right
        }
      } finally ctx.pop
    }
    def buildCols(cols: Cols): List[ColDef[_]] = {
      ctx push ColsCtx
      try if (cols != null) (cols.cols map {
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
      finally ctx.pop
    }
    lazy val builder: Transformer = transformer {
      case f: Fun => procedure(s"${f.name}#${f.parameters.size}").map { p =>
        val retType = if (p.returnTypeParIndex == -1) p.scalaReturnType else ManifestFactory.Nothing
        FunDef(p.name, f.copy(parameters = f.parameters map tr), retType, p)
      }.getOrElse(throw CompilerException(s"Unknown function: ${f.name}"))
      case c: Col =>
        val alias = if (c.alias != null) c.alias else c.col match {
          case Obj(Ident(name), _, _, _, _) => name.last //use last part of qualified ident as name
          case _ => null
        }
        ColDef(
          alias,
          tr(c.col) match { case x if ctx.head == ColsCtx => x case x: Exp => ChildDef(x) case x => throw CompilerException(s"Exp $x unsupported at this place")},
          if(c.typ != null) metadata.xsd_scala_type_map(c.typ) else ManifestFactory.Nothing
        )
      case Obj(b: Braces, _, _, _, _) if ctx.head == QueryCtx =>
        builder(b) //unwrap braces top level expression
      case o: Obj if ctx.head == QueryCtx | ctx.head == TablesCtx => //obj as query
        builder(Query(List(o), null, null, null, null, null, null))
      case o: Obj if ctx.head == BodyCtx =>
        o.copy(obj = builder(o.obj), join = builder(o.join).asInstanceOf[Join])
      case q: Query =>
        val tables = buildTables(q.tables)
        val cols = buildCols(q.cols)
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
            offset = offset))
      case b: BinOp =>
        (tr(b.lop), tr(b.rop)) match {
          case (lop: SelectDefBase, rop: SelectDefBase) =>
            BinSelectDef(lop, rop, b.copy(lop = lop, rop = rop))
          case (lop, rop) => b.copy(lop = lop, rop = rop)
        }
      case UnOp("|", o: Exp @unchecked) if ctx.head == ColsCtx =>
        val exp = try o match {
          //recursive expression
          case a: Arr =>
            ctx push BodyCtx
            RecursiveDef(builder(a))
          case e => //ordinary child
            ctx push QueryCtx
            builder(o)
        } finally ctx.pop
        ChildDef(exp)
      case Braces(exp: Exp) if ctx.head == TablesCtx => builder(exp) //remove braces around table expression, so it can be accessed directly
      case a: Arr if ctx.head == QueryCtx => ArrayDef(
        a.elements.zipWithIndex.map { case (el, idx) =>
          ColDef[Nothing](
            s"_${idx + 1}",
            tr(el) match {
              case r: RowDefBase => ChildDef(r)
              case e => e
            },
            ManifestFactory.Nothing)
        }
      )
      case dml: DMLExp =>
        val table = TableDef(dml.alias, Obj(TableObj(dml.table), null, null, null))
        ctx push ColsCtx
        val cols =
          if (dml.cols != null) dml.cols.map {
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
        val filter = if (dml.filter != null) tr(dml.filter).asInstanceOf[Arr] else null
        val vals = if (dml.vals != null) tr(dml.vals) else null
        ctx.pop
        dml match {
          case i: Insert =>
            InsertDef(cols, List(table), Insert(table = null, alias = null, cols = null, vals = vals))
          case u: Update =>
            UpdateDef(cols, List(table), Update(
              table = null, alias = null, cols = null, filter = filter, vals = vals))
          case d: Delete =>
            DeleteDef(List(table), Delete(table = null, alias = null, filter = filter))
        }
      case WithTable(name, wtCols, recursive, table) =>
        ctx push QueryCtx
          val tables: List[TableDef] = List(TableDef(name, Obj(Null, null, null, null, false)))
          val cols: List[ColDef[_]] = wtCols.map { c =>
            ColDef[Nothing](c, null, Manifest.Nothing)
          }
          val exp = builder(table) match {
            case s: SQLDefBase => s
            case x => throw CompilerException(s"Table in with clause must be query. Instead found: ${x.tresql}")
          }
        ctx.pop
        WithTableDef(cols, tables, recursive, exp)
      case With(tables, query) =>
        val withTables = (tables map builder).asInstanceOf[List[WithTableDef]]
        ctx push QueryCtx
        val exp = builder(query) match {
          case s: SelectDefBase => s
          case x => throw CompilerException(s"with clause must be select query. Instead found: ${x.tresql}")
        }
        ctx.pop
        WithSelectDef(exp, withTables)
      case null => null
    }
    builder(exp)
  }

  def resolveColAsterisks(exp: Exp) = {
    def createCol(col: String): Col = try {
      intermediateResults.get.clear
      column(new scala.util.parsing.input.CharSequenceReader(col)).get
    } finally intermediateResults.get.clear

    lazy val resolver: Transformer = transformer {
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
                val table = nsd.table(td.name).getOrElse(throw CompilerException(s"Cannot find table: $td"))
                table.cols.map { c =>
                  ColDef(c.name, createCol(s"${td.name}.${c.name}").col, c.scalaType)
                }
              }
            case ColDef(_, IdentAll(Ident(ident)), _) =>
              val alias = ident mkString "."
              nsd.table(alias)
                .map(_.cols.map { c =>
                  ColDef(c.name, createCol(s"$alias.${c.name}").col, c.scalaType)
                })
                .getOrElse(throw CompilerException(s"Cannot find table: $alias"))
            case cd @ ColDef(_, chd: ChildDef, _) =>
              List(cd.copy(col = resolver(chd)))
            case cd => List(cd)
          }
        }, exp = resolver(nsd.exp).asInstanceOf[Query])
    }
    resolver(exp)
  }

  def resolveScopes(exp: Exp) = {
    val scope_stack = scala.collection.mutable.Stack[Scope](thisCompiler)
    lazy val scoper: Transformer = transformer {
      case sd: SelectDef =>
        val nsd = sd.copy(parent = scope_stack.head)
        val t = (nsd.tables map scoper).asInstanceOf[List[TableDef]]
        scope_stack push nsd
        val c = (nsd.cols map scoper).asInstanceOf[List[ColDef[_]]]
        val q = scoper(nsd.exp).asInstanceOf[Query]
        scope_stack.pop
        nsd.copy(cols = c, tables = t, exp = q)
      case dml: DMLDefBase if !dml.isInstanceOf[InsertDef] && scope_stack.head != dml =>
        scope_stack push dml
        val ndml = scoper(dml)
        scope_stack.pop
        ndml
      case rd: RecursiveDef => rd.copy(scope = scope_stack.head)
    }
    scoper(exp)
  }

  def resolveNamesAndJoins(exp: Exp) = {
    trait Ctx
    object TableCtx extends Ctx
    object ColumnCtx extends Ctx
    case class Context(scope: Scope, ctx: Ctx)
    def checkDefaultJoin(scope: Scope, table1: TableDef, table2: TableDef) = if (table1 != null) {
      for {
        t1 <- scope.table(table1.name)
        t2 <- scope.table(table2.name)
      } yield try metadata.join(t1.name, t2.name) catch {
        case e: Exception => throw CompilerException(e.getMessage)
      }
    }
    lazy val namer: Extractor[Context] = extractorAndTraverser {
      case (ctx, sd: SelectDef) =>
        val nctx = ctx.copy(scope = sd) //create context with this select as a scope
        var prevTable: TableDef = null
        sd.tables foreach { t =>
          namer(ctx -> t.exp.obj) //table definition check goes within parent scope
          Option(t.exp.join).map { j =>
            //join definition check goes within this select scope
            namer(nctx -> j)
            j match {
              case Join(true, _, _) => checkDefaultJoin(sd, prevTable, t)
              case _ =>
            }
          }
          prevTable = t
        }
        sd.cols foreach (c => namer(nctx -> c))
        namer(nctx -> sd.exp)
        (ctx, false) //return old scope and stop traversing
      case (ctx, dml: DMLDefBase) =>
        dml.tables foreach (t => namer(ctx -> t.exp.obj))
        val nctx = ctx.copy(scope = dml)
        dml.cols foreach (c => namer(nctx -> c))
        dml match {
          case ins: InsertDef => namer(ctx -> ins.exp) //do not change scope for insert value clause name resolving
          case upd_del => namer(nctx -> upd_del.exp) //change scope for update delete filter and values name resolving
        }
        (ctx, false) //return old scope and stop traversing
      case (ctx, _: TableObj) => (ctx.copy(ctx = TableCtx), true) //set table context
      case (ctx, _: TableAlias) => (ctx, false) //do not check table alias is already checked
      case (ctx, _: Obj) => (ctx.copy(ctx = ColumnCtx), true) //set column context
      case (ctx @ Context(scope, TableCtx), Ident(ident)) => //check table
        val tn = ident mkString "."
        scope.table(tn).orElse(throw CompilerException(s"Unknown table: $tn"))
        (ctx, true)
      case (ctx @ Context(scope, ColumnCtx), Ident(ident)) => //check column
        val cn = ident mkString "."
        scope.column(cn).orElse(throw CompilerException(s"Unknown column: $cn"))
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
    lazy val typer: Extractor[Manifest[_]] = extractorAndTraverser {
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
          throw CompilerException(s"Select must contain only one column, instead:${s.cols.map(_.tresql).mkString(", ")}")
        else {
          scopes.push(s)
          val ret = (type_from_any(s.cols.head), false)
          scopes.pop
          ret
        }
      case (_, Ident(ident)) => (scopes.head.column(ident mkString ".").map(_.scalaType).get, false)
      case (_, f: FunDef[_]) =>
        (if (f.typ != null && f.typ != Manifest.Nothing) f.typ
        else if (f.procedure.returnTypeParIndex == -1) Manifest.Any
        else type_from_any(f.exp.parameters(f.procedure.returnTypeParIndex))) -> false
    }
    lazy val type_resolver: Transformer = transformer {
      case s: SelectDef =>
        //resolve column types for potential from clause select definitions
        val nsd = s.copy(tables = (s.tables map type_resolver).asInstanceOf[List[TableDef]])
        scopes.push(nsd)
        val ncols = (nsd.cols map type_resolver).asInstanceOf[List[ColDef[_]]] //resolve types for column defs
        scopes.pop
        nsd.copy(cols = ncols)
      case dml: DMLDefBase =>
        scopes.push(dml)
        val ncols = (dml.cols map type_resolver).asInstanceOf[List[ColDef[_]]]
        scopes.pop
        dml match {
          case ins: InsertDef => ins.copy(cols = ncols)
          case upd: UpdateDef => upd.copy(cols = ncols)
          case del: DeleteDef => del
        }
      case ColDef(n, ChildDef(ch), t) => ColDef(n, ChildDef(type_resolver(ch)), t)
      case ColDef(n, exp, typ) if typ == null || typ == Manifest.Nothing =>
        ColDef(n, exp, type_from_any(exp))
      case fd @ FunDef(n, f, typ, p) if typ == null || typ == Manifest.Nothing =>
        val t = if (p.returnTypeParIndex == -1) Manifest.Any else {
          type_from_any(f.parameters(p.returnTypeParIndex))
        }
        fd.copy(typ = t)
    }
    type_resolver(exp)
  }

  def compile(exp: Exp) = {
    resolveColTypes(
      resolveNamesAndJoins(
        resolveScopes(
          resolveColAsterisks(
            buildTypedDef(
              exp)))))
  }

  override def transformer(fun: Transformer): Transformer = {
    lazy val local_transformer = fun orElse traverse
    lazy val transform_traverse = local_transformer orElse super.transformer(local_transformer)
    def tr(x: Any): Any = x match {case e: Exp @unchecked => transform_traverse(e) case _ => x} //helper function
    lazy val traverse: Transformer = {
      case cd: ColDef[_] => cd.copy(col = tr(cd.col))
      case cd: ChildDef => cd.copy(exp = transform_traverse(cd.exp))
      case fd: FunDef[_] => fd.copy(exp = transform_traverse(fd.exp).asInstanceOf[Fun])
      case td: TableDef => td.copy(exp = transform_traverse(td.exp).asInstanceOf[Obj])
      case to: TableObj => to.copy(obj = transform_traverse(to.obj))
      case ta: TableAlias => ta.copy(obj = transform_traverse(ta.obj))
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
        val d = transform_traverse(dd.exp).asInstanceOf[Delete]
        DeleteDef(t, d)
      case ad: ArrayDef => ad.copy(cols = (ad.cols map transform_traverse).asInstanceOf[List[ColDef[_]]])
      case rd: RecursiveDef => rd.copy(exp = transform_traverse(rd.exp))
      case wtd: WithTableDef => wtd.copy(
        cols = (wtd.cols map transform_traverse).asInstanceOf[List[ColDef[_]]],
        tables = (wtd.tables map transform_traverse).asInstanceOf[List[TableDef]],
        exp = transform_traverse(wtd.exp).asInstanceOf[SQLDefBase]
      )
      case wsd: WithSelectDef => wsd.copy(
        exp = transform_traverse(wsd.exp).asInstanceOf[SelectDefBase],
        withTables = (wsd.tables map transform_traverse).asInstanceOf[List[WithTableDef]]
      )
    }
    transform_traverse
  }

  override def extractorAndTraverser[T](
    fun: ExtractorAndTraverser[T],
    traverser: Extractor[T] = PartialFunction.empty): Extractor[T] = {
    def tr(r: T, x: Any): T = x match {
      case e: Exp => extract_traverse((r, e))
      case l: List[_] => l.foldLeft(r) { (fr, el) => tr(fr, el) }
      case _ => r
    }
    lazy val extract_traverse: Extractor[T] =
      super.extractorAndTraverser(fun, traverser orElse local_extract_traverse)
    lazy val local_extract_traverse: Extractor[T] = {
      case (r: T @unchecked, cd: ColDef[_]) => tr(r, cd.col)
      case (r: T @unchecked, cd: ChildDef) => tr(r, cd.exp)
      case (r: T @unchecked, fd: FunDef[_]) => tr(r, fd.exp)
      case (r: T @unchecked, td: TableDef) => tr(r, td.exp)
      case (r: T @unchecked, to: TableObj) => tr(r, to.obj)
      case (r: T @unchecked, ta: TableAlias) => tr(r, ta.obj)
      case (r: T @unchecked, sd: SelectDef) => tr(tr(tr(r, sd.tables), sd.cols), sd.exp)
      case (r: T @unchecked, bd: BinSelectDef) => tr(tr(r, bd.leftOperand), bd.rightOperand)
      case (r: T @unchecked, id: InsertDef) => tr(tr(tr(r, id.tables), id.cols), id.exp)
      case (r: T @unchecked, ud: UpdateDef) => tr(tr(tr(r, ud.tables), ud.cols), ud.exp)
      case (r: T @unchecked, dd: DeleteDef) => tr(tr(tr(r, dd.tables), dd.cols), dd.exp)
      case (r: T @unchecked, ad: ArrayDef) => tr(r, ad.cols)
      case (r: T @unchecked, rd: RecursiveDef) => tr(r, rd.exp)
    }
    extract_traverse
  }

  def parseExp(expr: String): Any = try {
    intermediateResults.get.clear
    phrase(exprList)(new scala.util.parsing.input.CharSequenceReader(expr)) match {
      case Success(r, _) => r
      case x => throw CompilerException(x.toString, x.next.pos)
    }
  } finally intermediateResults.get.clear
}
