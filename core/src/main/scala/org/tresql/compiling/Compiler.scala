package org.tresql
package compiling

import org.tresql.parsing._
import org.tresql.metadata._
import org.tresql.Env
import scala.reflect.ManifestFactory

trait Scope {
  def tableNames: List[String]
  def table(table: String): Option[Table]
}

trait Compiler extends QueryParsers with ExpTransformer { thisCompiler =>

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
    override def tableNames = scope.tableNames
    override def table(table: String) = scope.table(table)
  }

  //is superclass of sql query and array
  trait RowDefBase extends TypedExp[RowDefBase] {
    def cols: List[ColDef[_]]
    val typ: Manifest[RowDefBase] = ManifestFactory.classType(this.getClass)
  }

  //superclass of select and dml statements (insert, update, delete)
  trait SQLDefBase extends RowDefBase with Scope {
    def tables: List[TableDef]

    def tableNames = tables.collect {
      //collect table names in this sql (i.e. exclude tresql no join aliases)
      case TableDef(t, Obj(_: TableObj, _, _, _, _)) => t.toLowerCase
    }
    def table(table: String) = tables.find(_.name.toLowerCase == table).flatMap {
      case TableDef(_, Obj(TableObj(Ident(name)), _, _, _, _)) =>
        Option(table_alias(name mkString "."))
      case TableDef(n, Obj(TableObj(s: SelectDefBase), _, _, _, _)) =>
        Option(table_from_selectdef(n, s))
      case x => throw CompilerException(
        s"Unrecognized table clause: '${x.tresql}'. Try using Query(...)")
    }
    protected def table_from_selectdef(name: String, sd: SelectDefBase) =
      Table(name, sd.cols map col_from_coldef, null, Map())
    protected def table_alias(name: String) = Table(name, Nil, null, Map())
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
    exp: Query
  ) extends SelectDefBase {
    //check for duplicating tables
    tables.filter { //filter out aliases
      case TableDef(_, Obj(_: TableAlias, _, _, _, _)) => false
      case _ => true
    }.groupBy(_.name).filter(_._2.size > 1) match {
      case d => if(d.size > 0) throw CompilerException(
        s"Duplicate table names: ${d.mkString(", ")}")
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
  }

  case class WithTableDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    recursive: Boolean,
    exp: SQLDefBase
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
    override def table(table: String) = tables.find(_.name == table).map {
      case _ => table_from_selectdef(table, this)
    }
  }

  case class WithSelectDef(
    exp: SelectDefBase,
    withTables: List[WithTableDef]
  ) extends SelectDefBase {
    def cols = exp.cols
    def tables = exp.tables
    override def table(table: String) = {
      def t(wts: List[WithTableDef]): Option[Table] = wts match {
        case Nil => None
        case wt :: tail => wt.table(table) orElse t(tail)
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

  //metadata
  /** Table of dml statement or table in from clause of any scope in the {{{scopes}}} list */
  def declaredTable(scopes: List[Scope])(tableName: String): Option[Table] = {
    val tn = tableName.toLowerCase
    scopes match {
      case Nil => None
      case scope :: tail => scope.table(tn).flatMap {
        case Table(n, c, _, _) if c.isEmpty => table(tail)(n) //alias is decoded ask parent scope
        case t => Some(t)
      } orElse declaredTable(tail)(tn)
    }
  }
  /** Table from metadata or defined in from clause of any scope in {{{scopes}}} list */
  def table(scopes: List[Scope])(tableName: String): Option[Table] = {
    val table = tableName.toLowerCase
    declaredTable(scopes)(table) orElse metadata.tableOption(table)
  }
  def column(scopes: List[Scope])(colName: String): Option[org.tresql.metadata.Col[_]] = {
    val col = colName.toLowerCase
    (scopes match {
      case Nil => None
      case scope :: tail => col.lastIndexOf('.') match {
        case -1 =>
          scope.tableNames
            .map(declaredTable(scopes)(_)
            .flatMap(_.colOption(col)))
            .collect { case col @ Some(_) => col } match {
              case List(col) => col
              case Nil => column(tail)(col)
              case x => throw CompilerException(s"Ambiguous columns: $x")
            }
        case x => declaredTable(scopes)(col.substring(0, x)).flatMap(_.colOption(col.substring(x + 1)))
      }
    }).orElse(procedure(s"$col#0").map(p => //check for empty pars function declaration
      org.tresql.metadata.Col(name = col, true, -1, scalaType = p.scalaReturnType)))
  }
  def procedure(procedure: String) = metadata.procedureOption(procedure)

  //expression def build
  def buildTypedDef(exp: Exp) = {
    trait Ctx
    object QueryCtx extends Ctx //root context
    object TablesCtx extends Ctx //from clause
    object ColsCtx extends Ctx //column clause
    object BodyCtx extends Ctx //where, group by, having, order, limit clauses

    //helper function
    def tr(ctx: Ctx, x: Any): Any = x match {case e: Exp @unchecked => builder(ctx)(e) case _ => x}
    def buildTables(tables: List[Obj]): List[TableDef] = {
      val td1 = tables.zipWithIndex map { case (table, idx) =>
        val newTable = builder(TablesCtx)(table.obj)
        val join = tr(BodyCtx, table.join).asInstanceOf[Join]
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
    }
    def buildCols(cols: Cols): List[ColDef[_]] = {
      if (cols != null) (cols.cols map {
          //child dml statement in select
          case c @ Col(_: DMLExp @unchecked, _, _) => builder(QueryCtx)(c)
          case c => builder(ColsCtx)(c)
        }).asInstanceOf[List[ColDef[_]]] match {
          case l if l.exists(_.name == null) => //set names of columns
            l.zipWithIndex.map { case (c, i) =>
              if (c.name == null) c.copy(name = s"_${i + 1}") else c
            }
          case l => l
        }
      else List[ColDef[_]](ColDef[Nothing](null, All, ManifestFactory.Nothing))
    }
    lazy val builder: TransformerWithState[Ctx] = transformerWithState(ctx => {
      case f: Fun => procedure(s"${f.name}#${f.parameters.size}").map { p =>
        val retType = if (p.returnTypeParIndex == -1) p.scalaReturnType else ManifestFactory.Nothing
        FunDef(p.name, f.copy(parameters = f.parameters map(tr(ctx, _))), retType, p)
      }.getOrElse(throw CompilerException(s"Unknown function: ${f.name}"))
      case c: Col =>
        val alias = if (c.alias != null) c.alias else c.col match {
          case Obj(Ident(name), _, _, _, _) => name.last //use last part of qualified ident as name
          case _ => null
        }
        ColDef(
          alias,
          tr(ctx, c.col) match {
            case x: DMLDefBase @unchecked => ChildDef(x)
            case x => x
          },
          if(c.typ != null) metadata.xsd_scala_type_map(c.typ) else ManifestFactory.Nothing
        )
      case Obj(b: Braces, _, _, _, _) if ctx == QueryCtx =>
        builder(ctx)(b) //unwrap braces top level expression
      case o: Obj if ctx == QueryCtx | ctx == TablesCtx => //obj as query
        builder(ctx)(Query(List(o), null, null, null, null, null, null))
      case o: Obj if ctx == BodyCtx =>
        o.copy(obj = builder(ctx)(o.obj), join = builder(ctx)(o.join).asInstanceOf[Join])
      case q: Query =>
        val tables = buildTables(q.tables)
        val cols = buildCols(q.cols)
        val (filter, grp, ord, limit, offset) =
          (tr(BodyCtx, q.filter).asInstanceOf[Filters],
           tr(BodyCtx, q.group).asInstanceOf[Grp],
           tr(BodyCtx, q.order).asInstanceOf[Ord],
           tr(BodyCtx, q.limit),
           tr(BodyCtx, q.offset))
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
        (tr(ctx, b.lop), tr(ctx, b.rop)) match {
          case (lop: SelectDefBase @unchecked, rop: SelectDefBase @unchecked) =>
            BinSelectDef(lop, rop, b.copy(lop = lop, rop = rop))
          case (lop, rop) => b.copy(lop = lop, rop = rop)
        }
      case UnOp("|", o: Exp @unchecked) if ctx == ColsCtx =>
        val exp = o match {
          //recursive expression
          case a: Arr => RecursiveDef(builder(BodyCtx)(a))
          //ordinary child
          case e => builder(QueryCtx)(o)
        }
        ChildDef(exp)
      case Braces(exp: Exp @unchecked) if ctx == TablesCtx => builder(ctx)(exp) //remove braces around table expression, so it can be accessed directly
      case a: Arr if ctx == QueryCtx => ArrayDef(
        a.elements.zipWithIndex.map { case (el, idx) =>
          ColDef[Nothing](
            s"_${idx + 1}",
            tr(ctx, el) match {
              case r: RowDefBase => ChildDef(r)
              case e => e
            },
            ManifestFactory.Nothing)
        }
      )
      case dml: DMLExp =>
        val table = TableDef(if (dml.alias == null) dml.table.ident mkString "." else dml.alias,
          Obj(TableObj(dml.table), null, null, null))
        val cols =
          if (dml.cols != null) dml.cols.map {
            case c @ Col(Obj(_: Ident, _, _, _, _), _, _) => builder(ColsCtx)(c) //insertable col
            case c => builder(QueryCtx)(c) //child expression
          }.asInstanceOf[List[ColDef[_]]]
          else Nil
        val filter = if (dml.filter != null) tr(BodyCtx, dml.filter).asInstanceOf[Arr] else null
        val vals = if (dml.vals != null) tr(BodyCtx, dml.vals) else null
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
        val exp = builder(QueryCtx)(table) match {
          case s: SQLDefBase => s
          case x => throw CompilerException(s"Table in with clause must be query. Instead found: ${x.tresql}")
        }
        val tables: List[TableDef] = List(TableDef(name, Obj(Null, null, null, null, false)))
        val cols: List[ColDef[_]] =
          if (wtCols.isEmpty) exp match {
            case sd: SelectDefBase => sd.cols
            case x => throw CompilerException(s"Unsupported with table definition: ${x.tresql}")
          } else wtCols.map { c =>
            ColDef[Nothing](c, Ident(List(c)), Manifest.Nothing)
          }
        WithTableDef(cols, tables, recursive, exp)
      case With(tables, query) =>
        val withTables = (tables map(builder(ctx)(_))).asInstanceOf[List[WithTableDef]]
        val exp = builder(QueryCtx)(query) match {
          case s: SelectDefBase => s
          case x => throw CompilerException(s"with clause must be select query. Instead found: ${x.tresql}")
        }
        WithSelectDef(exp, withTables)
      case null => null
    })
    builder(QueryCtx)(exp)
  }

  def resolveColAsterisks(exp: Exp) = {
    def createCol(col: String): Col = try {
      intermediateResults.get.clear
      column(new scala.util.parsing.input.CharSequenceReader(col)).get
    } finally intermediateResults.get.clear

    lazy val resolver: TransformerWithState[List[Scope]] = transformerWithState(scopes => {
      case sd: SelectDef =>
        val nsd = sd.copy(tables = {
          sd.tables.map {
            case td @ TableDef(_, Obj(TableObj(_: SelectDefBase), _, _, _, _)) =>
              resolver(scopes)(td).asInstanceOf[TableDef]
            case td => td
          }
        })
        val nscopes = nsd :: scopes
        nsd.copy (cols = {
          nsd.cols.flatMap {
            case ColDef(_, All, _) =>
              nsd.tables.flatMap { td =>
                table(nscopes)(td.name).map(_.cols.map { c =>
                  ColDef(c.name, createCol(s"${td.name}.${c.name}").col, c.scalaType)
                }).getOrElse(throw CompilerException(s"Cannot find table: $td"))
              }
            case ColDef(_, IdentAll(Ident(ident)), _) =>
              val alias = ident mkString "."
              table(nscopes)(alias)
                .map(_.cols.map { c => ColDef(c.name, createCol(s"$alias.${c.name}").col, c.scalaType) })
                .getOrElse(throw CompilerException(s"Cannot find table: $alias"))
            case cd @ ColDef(_, chd: ChildDef, _) =>
              List(cd.copy(col = resolver(nscopes)(chd)))
            case cd => List(cd)
          }
        }, exp = resolver(nscopes)(nsd.exp).asInstanceOf[Query])
    })
    resolver(Nil)(exp)
  }

  def resolveNamesAndJoins(exp: Exp) = {
    trait Ctx
    object TableCtx extends Ctx
    object ColumnCtx extends Ctx
    case class Context(scopes: List[Scope], ctx: Ctx)
    def checkDefaultJoin(scopes: List[Scope], table1: TableDef, table2: TableDef) = if (table1 != null) {
      for {
        t1 <- table(scopes)(table1.name)
        t2 <- table(scopes)(table2.name)
      } yield try metadata.join(t1.name, t2.name) catch {
        case e: Exception => throw CompilerException(e.getMessage)
      }
    }
    lazy val namer: Extractor[Context] = extractorAndTraverser {
      case (ctx, sd: SelectDef) =>
        val nctx = ctx.copy(scopes = sd :: ctx.scopes)
        var prevTable: TableDef = null
        sd.tables foreach { t =>
          namer(ctx -> t.exp.obj) //table definition check goes within parent scope
          Option(t.exp.join).map { j =>
            //join definition check goes within this select scope
            namer(nctx -> j)
            j match {
              case Join(true, _, _) => checkDefaultJoin(nctx.scopes, prevTable, t)
              case _ =>
            }
          }
          prevTable = t
        }
        sd.cols foreach (c => namer(nctx -> c))
        namer(nctx -> sd.exp)
        (ctx, false) //return old scope and stop traversing
      case (ctx, wtd: WithTableDef) if wtd.recursive =>
        namer(ctx.copy(scopes = wtd :: ctx.scopes) -> wtd.exp)
        (ctx, false)
      case (ctx, wsd: WithSelectDef) =>
        val nctx = ctx.copy(scopes = wsd :: ctx.scopes)
        wsd.withTables foreach { t => namer(nctx -> t) }
        namer(nctx -> wsd.exp)
        (ctx, false)
      case (ctx, dml: DMLDefBase) =>
        dml.tables foreach (t => namer(ctx -> t.exp.obj))
        val nctx = ctx.copy(scopes = dml :: ctx.scopes)
        dml.cols foreach (c => namer(nctx -> c))
        dml match {
          case ins: InsertDef => namer(ctx -> ins.exp) //do not change scope for insert value clause name resolving
          case upd_del => namer(nctx -> upd_del.exp) //change scope for update delete filter and values name resolving
        }
        (ctx, false) //return old scope and stop traversing
      case (ctx, _: TableObj) => (ctx.copy(ctx = TableCtx), true) //set table context
      case (ctx, _: TableAlias) => (ctx, false) //do not check table alias is already checked
      case (ctx, _: Obj) => (ctx.copy(ctx = ColumnCtx), true) //set column context
      case (ctx @ Context(scopes, TableCtx), Ident(ident)) => //check table
        val tn = ident mkString "."
        table(scopes)(tn).orElse(throw CompilerException(s"Unknown table: $tn"))
        (ctx, true)
      case (ctx @ Context(scopes, ColumnCtx), Ident(ident)) => //check column
        val cn = ident mkString "."
        column(scopes)(cn).orElse(throw CompilerException(s"Unknown column: $cn"))
        (ctx, true)
    }
    namer(Context(Nil, ColumnCtx) -> exp)
    exp
  }

  def resolveColTypes(exp: Exp) = {
    case class Ctx(scopes: List[Scope], mf: Manifest[_])
    def type_from_any(scopes: List[Scope], exp: Any) = Ctx(Nil, mf = exp match {
      case n: java.lang.Number => ManifestFactory.classType(n.getClass)
      case b: Boolean => ManifestFactory.Boolean
      case s: String => ManifestFactory.classType(s.getClass)
      /*null cannot be used since partial function does not match it as type T - Manifest*/
      case e: Exp @unchecked => typer((Ctx(scopes, Manifest.Nothing), e)).mf
      case x => ManifestFactory.classType(x.getClass)
    })
    lazy val typer: Extractor[Ctx] = extractorAndTraverser {
      case (Ctx(scopes, _), Ident(ident)) =>
        (Ctx(null, column(scopes)(ident mkString ".").map(_.scalaType).get), false)
      case (Ctx(scopes, _), UnOp(op, operand)) => (type_from_any(scopes, operand), false)
      case (Ctx(scopes, _), BinOp(op, lop, rop)) =>
        comp_op.findAllIn(op).toList match {
          case Nil =>
            val (lt, rt) = (type_from_any(scopes, lop).mf, type_from_any(scopes, rop).mf)
            val mf =
              if (lt.toString == "java.lang.String") lt else if (rt == "java.lang.String") rt
              else if (lt.toString == "java.lang.Boolean") lt else if (rt == "java.lang.Boolean") rt
              else if (lt <:< rt) rt else if (rt <:< lt) lt else lt
            (Ctx(null, mf), false)
          case _ => (Ctx(null, Manifest.Boolean), false)
        }
      case (_, _: TerOp) => (Ctx(null, Manifest.Boolean), false)
      case (Ctx(scopes, _), s: SelectDef) =>
        if (s.cols.size > 1)
          throw CompilerException(s"Select must contain only one column, instead:${s.cols.map(_.tresql).mkString(", ")}")
        else (type_from_any(s :: scopes, s.cols.head), false)
      case (Ctx(scopes, _), f: FunDef[_]) =>
        (if (f.typ != null && f.typ != Manifest.Nothing) Ctx(null, f.typ)
        else if (f.procedure.returnTypeParIndex == -1) Ctx(null, Manifest.Any)
        else type_from_any(scopes, f.exp.parameters(f.procedure.returnTypeParIndex))) -> false
    }
    lazy val type_resolver: TransformerWithState[List[Scope]] = transformerWithState(scopes => {
      case s: SelectDef =>
        //resolve column types for potential from clause select definitions
        val nsd = s.copy(tables = (s.tables map(type_resolver(scopes)(_))).asInstanceOf[List[TableDef]])
        //resolve types for column defs
        val nscopes = nsd :: scopes
        nsd.copy(cols = (nsd.cols map(type_resolver(nscopes)(_))).asInstanceOf[List[ColDef[_]]])
      case wtd: WithTableDef =>
        val nscopes = if (wtd.recursive) wtd :: scopes else scopes
        val exp = type_resolver(nscopes)(wtd.exp).asInstanceOf[SQLDefBase]
        val cols = wtd.cols zip exp.cols map { case (col, ecol) => col.copy(typ = ecol.typ) }
        wtd.copy(cols = cols, exp = exp)
      case wsd: WithSelectDef =>
        val wt = (wsd.withTables map(type_resolver(scopes)(_))).asInstanceOf[List[WithTableDef]]
        wsd.copy(withTables = wt, exp = type_resolver(wsd :: scopes)(wsd.exp).asInstanceOf[SelectDefBase])
      case dml: DMLDefBase =>
        val nscopes = dml :: scopes
        val ncols = (dml.cols map(type_resolver(nscopes)(_))).asInstanceOf[List[ColDef[_]]]
        dml match {
          case ins: InsertDef => ins.copy(cols = ncols)
          case upd: UpdateDef => upd.copy(cols = ncols)
          case del: DeleteDef => del
        }
      case ColDef(n, ChildDef(ch), t) => ColDef(n, ChildDef(type_resolver(scopes)(ch)), t)
      case ColDef(n, exp, typ) if typ == null || typ == Manifest.Nothing =>
        ColDef(n, exp, type_from_any(scopes, exp).mf)
      case fd @ FunDef(n, f, typ, p) if typ == null || typ == Manifest.Nothing =>
        val t = if (p.returnTypeParIndex == -1) Manifest.Any else {
          type_from_any(scopes, f.parameters(p.returnTypeParIndex)).mf
        }
        fd.copy(typ = t)
    })
    type_resolver(Nil)(exp)
  }

  def compile(exp: Exp) = {
    resolveColTypes(
      resolveNamesAndJoins(
        resolveColAsterisks(
          buildTypedDef(
            exp))))
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
        withTables = (wsd.withTables map transform_traverse).asInstanceOf[List[WithTableDef]]
      )
    }
    transform_traverse
  }

  override def transformerWithState[T](fun: TransformerWithState[T]): TransformerWithState[T] = {
    def local_transformer(state: T): Transformer = fun(state) orElse traverse(state)
    def transform_traverse(state: T): Transformer = {
      val lt = local_transformer(state)
      lt orElse super.transformer(lt)
    }
    //helper function
    def tr(state: T, x: Any): Any = x match {
      case e: Exp @unchecked => transform_traverse(state)(e)
      case _ => x
    }
    def traverse(state: T): Transformer = {
      case cd: ColDef[_] => cd.copy(col = tr(state, cd.col))
      case cd: ChildDef => cd.copy(exp = transform_traverse(state)(cd.exp))
      case fd: FunDef[_] => fd.copy(exp = transform_traverse(state)(fd.exp).asInstanceOf[Fun])
      case td: TableDef => td.copy(exp = transform_traverse(state)(td.exp).asInstanceOf[Obj])
      case to: TableObj => to.copy(obj = transform_traverse(state)(to.obj))
      case ta: TableAlias => ta.copy(obj = transform_traverse(state)(ta.obj))
      case sd: SelectDef =>
        val t = (sd.tables map(transform_traverse(state)(_))).asInstanceOf[List[TableDef]]
        val c = (sd.cols map(transform_traverse(state)(_))).asInstanceOf[List[ColDef[_]]]
        val q = transform_traverse(state)(sd.exp).asInstanceOf[Query]
        sd.copy(cols = c, tables = t, exp = q)
      case bd: BinSelectDef => bd.copy(
        leftOperand = transform_traverse(state)(bd.leftOperand).asInstanceOf[SelectDefBase],
        rightOperand = transform_traverse(state)(bd.rightOperand).asInstanceOf[SelectDefBase])
      case id: InsertDef =>
        val t = (id.tables map(transform_traverse(state)(_))).asInstanceOf[List[TableDef]]
        val c = (id.cols map(transform_traverse(state)(_))).asInstanceOf[List[ColDef[_]]]
        val i = transform_traverse(state)(id.exp).asInstanceOf[Insert]
        InsertDef(c, t, i)
      case ud: UpdateDef =>
        val t = (ud.tables map(transform_traverse(state)(_))).asInstanceOf[List[TableDef]]
        val c = (ud.cols map(transform_traverse(state)(_))).asInstanceOf[List[ColDef[_]]]
        val u = transform_traverse(state)(ud.exp).asInstanceOf[Update]
        UpdateDef(c, t, u)
      case dd: DeleteDef =>
        val t = (dd.tables map(transform_traverse(state)(_))).asInstanceOf[List[TableDef]]
        val d = transform_traverse(state)(dd.exp).asInstanceOf[Delete]
        DeleteDef(t, d)
      case ad: ArrayDef => ad.copy(
        cols = (ad.cols map(transform_traverse(state)(_))).asInstanceOf[List[ColDef[_]]])
      case rd: RecursiveDef => rd.copy(exp = transform_traverse(state)(rd.exp))
      case wtd: WithTableDef => wtd.copy(
        cols = (wtd.cols map(transform_traverse(state)(_))).asInstanceOf[List[ColDef[_]]],
        tables = (wtd.tables map(transform_traverse(state)(_))).asInstanceOf[List[TableDef]],
        exp = transform_traverse(state)(wtd.exp).asInstanceOf[SQLDefBase]
      )
      case wsd: WithSelectDef => wsd.copy(
        exp = transform_traverse(state)(wsd.exp).asInstanceOf[SelectDefBase],
        withTables = (wsd.withTables map(transform_traverse(state)(_))).asInstanceOf[List[WithTableDef]]
      )
    }
    transform_traverse _
  }

  override def extractorAndTraverser[T](
    fun: ExtractorAndTraverser[T],
    traverser: Extractor[T] = PartialFunction.empty): Extractor[T] = {
    def tr(r: T, x: Any): T = x match {
      case e: Exp @unchecked => extract_traverse((r, e))
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
      case (r: T @unchecked, wtd: WithTableDef) => tr(r, wtd.exp)
      case (r: T @unchecked, wsd: WithSelectDef) => tr(tr(r, wsd.withTables), wsd.exp)
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
