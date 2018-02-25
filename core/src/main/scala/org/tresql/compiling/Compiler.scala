package org.tresql
package compiling

import org.tresql.parsing._
import org.tresql.metadata._
import scala.reflect.ManifestFactory

trait Compiler extends QueryParsers with ExpTransformer { thisCompiler =>

  case class CompilerException(
    message: String,
    pos: scala.util.parsing.input.Position = scala.util.parsing.input.NoPosition
  ) extends Exception(message)

  val metadata = Env.metadata

  trait Scope {
    def tableNames: List[String]
    def table(table: String): Option[Table]
    def column(col: String): Option[org.tresql.metadata.Col[_]] = None
  }

  trait TypedExp[T] extends Exp {
    def exp: Exp
    def typ: Manifest[T]
    def tresql = exp.tresql
  }

  case class TableDef(name: String, exp: Obj) extends Exp { def tresql = exp.tresql }
  /** helper class for namer to distinguish table references from column references */
  case class TableObj(obj: Exp) extends Exp {
    def tresql = obj.tresql
  }
  /** helper class for namer to distinguish table with NoJoin, i.e. must be defined in tables clause earlier */
  case class TableAlias(obj: Exp) extends Exp {
    def tresql = obj.tresql
  }
  case class ColDef[T](name: String, col: Exp, typ: Manifest[T]) extends TypedExp[T] {
    def exp = this
    override def tresql = Col(col, name).tresql
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
      case TableDef(_, Obj(TableObj(Null), _, _, _,_)) => Option(table_alias(null))
      case x => throw CompilerException(
        s"Unrecognized table clause: '${x.tresql}'. Try using Query(...)")
    }
    protected def table_from_selectdef(name: String, sd: SelectDefBase) =
      Table(name, sd.cols map col_from_coldef, null, Map())
    protected def table_alias(name: String) = Table(name, Nil, null, Map())
    protected def col_from_coldef(cd: ColDef[_]) =
      org.tresql.metadata.Col(name = cd.name, true, -1, scalaType = cd.typ)

    override def tresql = exp match {
      case q: Query =>
        /* FIXME distinct keyword is lost in Cols */
        q.copy(tables = this.tables.map(_.exp), cols = Cols(false, this.cols.map(c => Col(c.exp, c.name)))).tresql
    }
  }

  //is superclass of insert, update, delete
  trait DMLDefBase extends SQLDefBase

  //is superclass of select, union, intersect etc.
  trait SelectDefBase extends SQLDefBase {
    override def column(col: String) = cols.find(_.name == col) map col_from_coldef
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
      case d => if(d.nonEmpty) throw CompilerException(
        s"Duplicate table names: ${d.mkString(", ")}")
    }
  }

  //union, intersect, except ...
  case class BinSelectDef(
    leftOperand: SelectDefBase,
    rightOperand: SelectDefBase,
    exp: BinOp) extends SelectDefBase {
    private val op =
      if (rightOperand.isInstanceOf[FunSelectDef]) leftOperand
      else if (leftOperand.isInstanceOf[FunSelectDef]) rightOperand
      else leftOperand
    if (!(leftOperand.cols.exists {
        case ColDef(_, All | _: IdentAll, _) => true
        case _ => false
      } || rightOperand.cols.exists {
        case ColDef(_, All | _: IdentAll, _) => true
        case _ => false
      } || leftOperand.isInstanceOf[FunSelectDef]
        || rightOperand.isInstanceOf[FunSelectDef]
    ) && leftOperand.cols.size != rightOperand.cols.size)
      throw CompilerException(
        s"Column count do not match ${leftOperand.cols.size} != ${rightOperand.cols.size}")
    def cols = op.cols
    def tables = op.tables
  }

  //delegates all calls to inner expression
  case class BracesSelectDef(exp: SelectDefBase) extends SelectDefBase {
    override def cols = exp.cols
    override def tables = exp.tables
    override def tableNames = exp.tableNames
    override def table(table: String) = exp.table(table)
    override def column(col: String) = exp.column(col)
  }

  // table definition in with [recursive] statement
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
    if (cols.nonEmpty && cols.size != exp.cols.size)
      throw CompilerException(s"with table definition column count must equal corresponding query definition column count: ${exp.tresql}")
    override def table(table: String) = tables.find(_.name == table).map {
      case _ => table_from_selectdef(table, this)
    }
  }

  case class ValuesFromSelectDef(exp: SelectDefBase) extends SelectDefBase {
    override def cols = exp.cols
    override def tables = exp.tables
    override def tableNames = exp.tableNames
    override def table(table: String) = exp.table(table)
    override def column(col: String) = exp.column(col)
  }

  /** select definition returned from macro or db function, is used in {{{BinSelectDef}}} */
  case class FunSelectDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: FunDef[_]
  ) extends SelectDefBase

  case class InsertDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: Insert
  ) extends DMLDefBase {
    override def tresql = // FIXME alias lost
      exp.copy(table = Ident(List(tables.head.name)), cols = this.cols.map(c => Col(c.exp, c.name))).tresql
  }

  case class UpdateDef(
    cols: List[ColDef[_]],
    tables: List[TableDef],
    exp: Update
  ) extends DMLDefBase {
    override def tresql = // FIXME alias lost
      exp.copy(table = Ident(List(tables.head.name)), cols = this.cols.map(c => Col(c.exp, c.name))).tresql
  }

  case class DeleteDef(
    tables: List[TableDef],
    exp: Delete
  ) extends DMLDefBase {
    def cols = Nil
    override def tresql = // FIXME alias lost
      exp.copy(table = Ident(List(tables.head.name))).tresql
  }

  // with [recursive] expressions
  trait WithQuery extends SQLDefBase {
    def exp: SQLDefBase
    def withTables: List[WithTableDef]
  }
  trait WithSelectBase extends SelectDefBase with WithQuery {
    def exp: SelectDefBase
  }
  trait WithDMLQuery extends DMLDefBase with WithQuery {
    def exp: DMLDefBase
  }

  case class WithSelectDef(
    exp: SelectDefBase,
    withTables: List[WithTableDef]
  ) extends WithSelectBase {
    def cols = exp.cols
    def tables = exp.tables
    override def table(table: String) = exp.table(table)
  }

  case class WithDMLDef(
    exp: DMLDefBase,
    withTables: List[WithTableDef]
  ) extends DMLDefBase with WithQuery {
    def cols = exp.cols
    def tables = exp.tables
  }

  //array
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
  /** Table from metadata or defined in from clause or dml table of any scope in {{{scopes}}} list */
  def table(scopes: List[Scope])(tableName: String): Option[Table] = Option(tableName).flatMap { tn =>
    val table = tn.toLowerCase
    declaredTable(scopes)(table) orElse metadata.tableOption(table)
  }
  /** Column declared in any scope in {{{scopes}}} list */
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
  /** Method is used to resolve column names in group by or order by clause, since they can reference columns by name from column clause. */
  def declaredColumn(scopes: List[Scope])(colName: String): Option[org.tresql.metadata.Col[_]] = {
    val col = colName.toLowerCase
    scopes.head.column(col) orElse column(scopes)(col)
  }
  def procedure(procedure: String) = metadata.procedureOption(procedure)

  def buildTypedDef(exp: Exp) = {
    trait Ctx
    object QueryCtx extends Ctx //root context
    object TablesCtx extends Ctx //from clause
    object ColsCtx extends Ctx //column clause
    object BodyCtx extends Ctx //where, group by, having, order, limit clauses

    //helper function
    def tr(ctx: Ctx, x: Exp): Exp = builder(ctx)(x)
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
          case c @ Col(_: DMLExp @unchecked, _) => builder(QueryCtx)(c)
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
        FunDef(p.name, f.copy(
          parameters = f.parameters map(tr(ctx, _)),
          aggregateOrder = f.aggregateOrder.map(tr(ctx, _).asInstanceOf[Ord]),
          aggregateWhere = f.aggregateWhere.map(tr(ctx, _))
        ), retType, p)
      }.getOrElse(throw CompilerException(s"Unknown function: ${f.name}"))
      case c: Col =>
        val alias = if (c.alias != null) c.alias else c.col match {
          case Obj(Ident(name), _, _, _, _) => name.last //use last part of qualified ident as name
          case _ => null
        }
        ColDef[Nothing](
          alias,
          tr(ctx, c.col) match {
            case x: DMLDefBase @unchecked => ChildDef(x)
            case x => x
          },
          ManifestFactory.Nothing
        )
      case Obj(b: Braces, _, _, _, _) if ctx == QueryCtx =>
        builder(ctx)(b) //unwrap braces top level expression
      case o: Obj if ctx == QueryCtx | ctx == TablesCtx => //obj as query
        builder(ctx)(Query(List(o), Filters(Nil), null, null, null, null, null))
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
            BinSelectDef(lop, rop, b)
          //asume left operand select definition is provided by db function or macro
          case (lop: FunDef[_] @unchecked, rop: SelectDefBase @unchecked) =>
            BinSelectDef(FunSelectDef(Nil, Nil, lop), rop, b)
          //asume right operand select definition is provided by db function or macro
          case (lop: SelectDefBase @unchecked, rop: FunDef[_] @unchecked) =>
            BinSelectDef(lop, FunSelectDef(Nil, Nil, rop), b)
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
      case Braces(exp: Exp @unchecked) => builder(ctx)(exp) match {
        case sdb: SelectDefBase => BracesSelectDef(sdb)
        case e => Braces(e)
      }
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
            case c @ Col(Obj(_: Ident, _, _, _, _), _) => builder(ColsCtx)(c) //insertable, updatable col
            case c @ Col(BinOp("=", Obj(_: Ident, _, _, _, _), _), _) if dml.isInstanceOf[Update] => builder(ColsCtx)(c) //updatable col
            case c => builder(QueryCtx)(c) //child expression
          }.asInstanceOf[List[ColDef[_]]]
          else Nil
        val filter = if (dml.filter != null) tr(BodyCtx, dml.filter).asInstanceOf[Arr] else null
        val vals = if (dml.vals != null) tr(BodyCtx, dml.vals) else null
        dml match {
          case i: Insert =>
            InsertDef(cols, List(table), Insert(table = null, alias = null, cols = Nil, vals = vals))
          case u: Update =>
            UpdateDef(cols, List(table), Update(
              table = null, alias = null, cols = Nil, filter = filter, vals = vals))
          case d: Delete =>
            DeleteDef(List(table), Delete(table = null, alias = null, filter = filter))
        }
      case ValuesFromSelect(sel) =>
        ValuesFromSelectDef(tr(QueryCtx, sel).asInstanceOf[SelectDefBase])
      case WithTable(name, wtCols, recursive, table) =>
        val exp = builder(QueryCtx)(table) match {
          case s: SQLDefBase => s
          case x => throw CompilerException(s"Table in with clause must be query. Instead found: ${x.getClass.getName}(${x.tresql})")
        }
        val tables: List[TableDef] = List(TableDef(name, Obj(TableObj(Ident(List(name))), null, null, null, false)))
        val cols: List[ColDef[_]] =
          if (wtCols.isEmpty) exp match {
            case sd: SelectDefBase => sd.cols
            case x => throw CompilerException(s"Unsupported with table definition: ${x.tresql}")
          } else wtCols.map { c =>
            ColDef[Nothing](c, Ident(List(c)), Manifest.Nothing)
          }
        WithTableDef(cols, tables, recursive, exp)
      case With(tables, query) =>
        val withTables = (tables map builder(ctx)).asInstanceOf[List[WithTableDef]]
        builder(QueryCtx)(query) match {
          case s: SelectDefBase => WithSelectDef(s, withTables)
          case i: DMLDefBase => WithDMLDef(i, withTables)
          case x => throw CompilerException(s"with clause must be select query. Instead found: ${x.getClass.getName}(${x.tresql})")
        }
      case null => null
    })
    builder(QueryCtx)(exp)
  }

  def resolveColAsterisks(exp: Exp) = {
    def createCol(col: String): Col = try {
      intermediateResults.get.clear
      column(new scala.util.parsing.input.CharSequenceReader(col)).get
    } finally intermediateResults.get.clear

    lazy val resolver: TransformerWithState[List[Scope]] = transformerWithState { scopes =>
      def resolveWithQuery[T <: SQLDefBase](withQuery: WithQuery): (T, List[WithTableDef]) = {
        val wtables: List[WithTableDef] = withQuery.withTables.foldLeft(List[WithTableDef]()) { (tables, table) =>
          val withScopes = tables ++ scopes
          (resolver(if (table.recursive) table :: withScopes else withScopes)(table) match {
            case wtd @ WithTableDef(List(ColDef(_, All, _)), _, _, _) =>
              wtd.copy(cols = wtd.exp.cols.map(col =>
                ColDef(
                  col.name,
                  Obj(Ident(List(wtd.tables.head.name, col.name)), null, null, null, false),
                  col.typ
                )))
            case x: WithTableDef => x
            case x => sys.error(s"Compiler error, expected WithTableDef, encountered: $x")
          }) :: tables
        }
        val exp = resolver(wtables ++ scopes)(withQuery.exp).asInstanceOf[T]
        (exp, wtables.reverse)
      }
      {
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
              case ColDef(_, All, _) => nsd.tables.flatMap { td =>
                table(nscopes)(td.name).map(_.cols.map { c =>
                  ColDef(c.name, createCol(s"${td.name}.${c.name}").col, c.scalaType)
                }).getOrElse(throw CompilerException(s"Cannot find table: ${td.tresql}\nScopes:\n$nscopes"))
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
        case wsd: WithSelectDef =>
          val (exp, wtables) = resolveWithQuery[SelectDefBase](wsd)
          wsd.copy(exp, wtables)
        case wdmld: WithDMLDef =>
          val (exp, wtables) = resolveWithQuery[DMLDefBase](wdmld)
          wdmld.copy(exp, wtables)
      }
    }
    resolver(Nil)(exp)
  }

  def resolveNamesAndJoins(exp: Exp) = {
    trait Ctx
    object TableCtx extends Ctx
    object ColumnCtx extends Ctx
    //isGrpOrd indicates group, order clauses where column clause column aliases can be referenced
    case class Context(colScopes: List[Scope], tblScopes: List[Scope], ctx: Ctx, isGrpOrd: Boolean)
    def checkDefaultJoin(scopes: List[Scope], table1: TableDef, table2: TableDef) = if (table1 != null) {
      for {
        t1 <- table(scopes)(table1.name)
        t2 <- table(scopes)(table2.name)
      } yield try metadata.join(t1.name, t2.name) catch {
        case e: Exception => throw CompilerException(e.getMessage)
      }
    }
    lazy val namer: Traverser[Context] = traverser(ctx => {
      case sd: SelectDef =>
        val nctx = ctx.copy(colScopes = sd :: ctx.colScopes, isGrpOrd = false)
        var prevTable: TableDef = null
        sd.tables foreach { t =>
          namer(ctx)(t.exp.obj) //table definition check goes within parent scope
          Option(t.exp.join).map { j =>
            //join definition check goes within this select scope
            namer(nctx)(j)
            j match {
              case Join(true, _, _) => checkDefaultJoin(nctx.tblScopes, prevTable, t)
              case _ =>
            }
          }
          prevTable = t
        }
        sd.cols foreach namer(nctx)
        namer(nctx)(sd.exp.filter)
        val grpOrdCtx = nctx.copy(isGrpOrd = true)
        Option(sd.exp.group).foreach { g =>
          g.cols.foreach(namer(grpOrdCtx))
          namer(nctx)(g.having)
        }
        namer(grpOrdCtx)(sd.exp.order)
        namer(nctx)(sd.exp.offset)
        namer(nctx)(sd.exp.limit)
        //return old scope
        ctx
      case wtd: WithTableDef if wtd.recursive =>
        namer(ctx.copy(colScopes = wtd :: ctx.colScopes, tblScopes = wtd :: ctx.tblScopes))(wtd.exp)
        ctx
      case wsd: WithQuery =>
        val wtCtx = wsd.withTables.foldLeft(ctx) { (ctx, table) =>
          namer(ctx)(table)
          ctx.copy(colScopes = table :: ctx.colScopes, tblScopes = table :: ctx.tblScopes)
        }
        namer(wtCtx)(wsd.exp)
        ctx
      case dml: DMLDefBase =>
        val dmlTableCtx = Context(Nil, Nil, TableCtx, isGrpOrd = false) // root scope
        val dmlColCtx = Context(colScopes = List(dml), tblScopes = Nil, ColumnCtx, isGrpOrd = false) // dml table scope
        val thisCtx = ctx.copy(colScopes = dml :: ctx.colScopes)
        dml.tables foreach (t => namer(dmlTableCtx)(t.exp.obj)) // dml table must come from root scope
        dml match {
          case ins: InsertDef =>
            dml.cols foreach namer(dmlColCtx) // dml columns must come from dml table
            namer(ctx)(ins.exp) //do not change scope for insert value clause name resolving
          case upd: UpdateDef => upd.exp.vals match {
            case vfsd: ValuesFromSelectDef =>
              namer(ctx)(vfsd) //vals clause
              val colsCtx = thisCtx.copy(colScopes = vfsd :: thisCtx.colScopes)
              upd.cols foreach {
                // column must consist of assignement operation, on left side of which must be updateable column
                case ColDef(_, BinOp("=" ,lop @ Obj(Ident(_), _, _, _, _), rop), _) =>
                  namer(dmlColCtx)(lop) //updateable column must come from update statement table
                  namer(colsCtx)(rop)
                case x => throw CompilerException(s"For updateable column clause expected assignement operation to column. Instead found: $x")
              }
              namer(colsCtx)(upd.exp.filter) //filter clause
            case vals =>
              dml.cols foreach namer(dmlColCtx) // dml columns must come from dml table
              namer(thisCtx)(upd.exp) //filters and vals check with this context
          }
          case del: DeleteDef => namer(thisCtx)(del.exp) // delete filter name resolving
        }
        //return old scope
        ctx
      case t: TableObj => namer(ctx.copy(ctx = TableCtx))(t.obj) //set table context
      case _: TableAlias => ctx //do not check table alias is already checked
      case o: Obj => namer(namer(ctx.copy(ctx = ColumnCtx))(o.join))(o.obj) //set column context
      case Ident(ident) if ctx.ctx == TableCtx => //check table
        val tn = ident mkString "."
        table(ctx.tblScopes)(tn).orElse(throw CompilerException(s"Unknown table: $tn"))
        ctx
      case Ident(ident) if ctx.ctx == ColumnCtx => //check column
        val cn = ident mkString "."
        (if (ctx.isGrpOrd) declaredColumn(ctx.colScopes)(cn) else column(ctx.colScopes)(cn))
          .orElse(throw CompilerException(s"Unknown column: $cn"))
        ctx
    })
    namer(Context(Nil, Nil, ColumnCtx, isGrpOrd = false))(exp)
    exp
  }

  def resolveColTypes(exp: Exp) = {
    case class Ctx(scopes: List[Scope], mf: Manifest[_])
    def type_from_const(const: Any): Ctx = Ctx(Nil, const match {
      case n: java.lang.Number => ManifestFactory.classType(n.getClass)
      case b: Boolean => ManifestFactory.Boolean
      case s: String => ManifestFactory.classType(s.getClass)
      case m: Manifest[_] => m
      case null => Manifest.Any
      case x => ManifestFactory.classType(x.getClass)
    })
    def type_from_exp(ctx: Ctx, exp: Exp) = typer(ctx)(exp)
    lazy val typer: Traverser[Ctx] = traverser(ctx => {
      case Const(const) => type_from_const(const)
      case Null => type_from_const(null)
      case Ident(ident) =>
        Ctx(ctx.scopes, column(ctx.scopes)(ident mkString ".").map(_.scalaType).get)
      case UnOp(_, operand) => type_from_exp(ctx, operand)
      case BinOp(op, lop, rop) =>
        comp_op.findAllIn(op).toList match {
          case Nil =>
            val (lt, rt) = (type_from_exp(ctx, lop).mf, type_from_exp(ctx, rop).mf)
            val mf =
              if (lt.toString == "java.lang.String") lt else if (rt.toString == "java.lang.String") rt
              else if (lt.toString == "java.lang.Boolean") lt else if (rt.toString == "java.lang.Boolean") rt
              else if (lt <:< rt) rt else if (rt <:< lt) lt else lt
            Ctx(ctx.scopes, mf)
          case _ => type_from_const(true)
        }
      case _: TerOp => type_from_const(true)
      case _: In => type_from_const(true)
      case s: SelectDefBase =>
        if (s.cols.size > 1)
          throw CompilerException(s"Select must contain only one column, instead:${
            s.cols.map(_.tresql).mkString(", ")}")
        else type_from_const(s.cols.head.typ)
      case f: FunDef[_] =>
        if (f.typ != null && f.typ != Manifest.Nothing) type_from_const(f.typ)
        else if (f.procedure.returnTypeParIndex == -1) type_from_const(Manifest.Any)
        else type_from_exp(ctx, f.exp.parameters(f.procedure.returnTypeParIndex))
    })
    lazy val type_resolver: TransformerWithState[List[Scope]] = transformerWithState { ctx =>
      def resolveWithQuery[T <: SQLDefBase](withQuery: WithQuery): (T, List[WithTableDef]) = {
        val wtables = withQuery.withTables.foldLeft(List[WithTableDef]()) { (tables, table) =>
          type_resolver(tables ++ ctx)(table).asInstanceOf[WithTableDef] :: tables
        }
        (type_resolver(wtables ++ ctx)(withQuery.exp).asInstanceOf[T], wtables.reverse)
      }
      {
        case s: SelectDef =>
          //resolve column types for potential from clause select definitions
          val nsd = s.copy(tables = (s.tables map type_resolver(s :: ctx)).asInstanceOf[List[TableDef]])
          //resolve types for column defs
          nsd.copy(cols = (nsd.cols map type_resolver(nsd :: ctx)).asInstanceOf[List[ColDef[_]]])
        case wtd: WithTableDef =>
          val nctx = if (wtd.recursive) wtd :: ctx else ctx
          val exp = type_resolver(nctx)(wtd.exp).asInstanceOf[SQLDefBase]
          val cols = wtd.cols zip exp.cols map { case (col, ecol) => col.copy(typ = ecol.typ) }
          wtd.copy(cols = cols, exp = exp)
        case wsd: WithSelectDef =>
          val (exp, wtables) = resolveWithQuery[SelectDefBase](wsd)
          wsd.copy(exp = exp, withTables = wtables)
        case wdmld: WithDMLDef =>
          val (exp, wtables) = resolveWithQuery[DMLDefBase](wdmld)
          wdmld.copy(exp = exp, withTables = wtables)
        case dml: DMLDefBase if ctx.isEmpty || ctx.head != dml => type_resolver(dml :: ctx)(dml)
        case ColDef(n, ChildDef(ch), t) => ColDef(n, ChildDef(type_resolver(ctx)(ch)), t)
        case ColDef(n, exp, typ) if typ == null || typ == Manifest.Nothing =>
          val nexp = type_resolver(ctx)(exp) //resolve expression in the case it contains select
          ColDef(n, nexp, typer(Ctx(ctx, Manifest.Any))(nexp).mf)
        //resolve return type only for root level function
        case fd @ FunDef(n, f, typ, p) if ctx.isEmpty && (typ == null || typ == Manifest.Nothing) =>
          val t = if (p.returnTypeParIndex == -1) Manifest.Any else {
            typer(Ctx(ctx, Manifest.Any))(f.parameters(p.returnTypeParIndex)).mf
          }
          fd.copy(typ = t)
      }
    }
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
    def tt[A <: Exp](exp: A) = transform_traverse(exp).asInstanceOf[A]
    lazy val traverse: Transformer = {
      case cd: ColDef[_] => cd.copy(col = tt(cd.col))
      case cd: ChildDef => cd.copy(exp = tt(cd.exp))
      case fd: FunDef[_] => fd.copy(exp = tt(fd.exp))
      case td: TableDef => td.copy(exp = tt(td.exp))
      case to: TableObj => to.copy(obj = tt(to.obj))
      case ta: TableAlias => ta.copy(obj = tt(ta.obj))
      case sd: SelectDef =>
        val t = sd.tables map tt
        val c = sd.cols map tt
        val q = tt(sd.exp)
        sd.copy(cols = c, tables = t, exp = q)
      case bd: BinSelectDef => bd.copy(
        leftOperand = tt(bd.leftOperand),
        rightOperand = tt(bd.rightOperand)
      )
      case id: InsertDef =>
        val t = id.tables map tt
        val c = id.cols map tt
        val i = tt(id.exp)
        InsertDef(c, t, i)
      case ud: UpdateDef =>
        val t = ud.tables map tt
        val c = ud.cols map tt
        val u = tt(ud.exp)
        UpdateDef(c, t, u)
      case dd: DeleteDef => DeleteDef(dd.tables map tt, tt(dd.exp))
      case ad: ArrayDef => ad.copy(cols = ad.cols map tt)
      case rd: RecursiveDef => rd.copy(exp = tt(rd.exp))
      case wtd: WithTableDef => wtd.copy(
        cols = wtd.cols map tt,
        tables = wtd.tables map tt,
        exp = tt(wtd.exp)
      )
      case wsd: WithSelectDef => WithSelectDef(tt(wsd.exp), wsd.withTables map tt)
      case wdml: WithDMLDef => WithDMLDef(tt(wdml.exp), wdml.withTables map tt)
      case BracesSelectDef(sdb) => BracesSelectDef(tt(sdb))
      case fsd: FunSelectDef => fsd.copy(exp = tt(fsd.exp))
      case vfsd: ValuesFromSelectDef => vfsd.copy(exp = tt(vfsd.exp))
    }
    transform_traverse
  }

  override def transformerWithState[T](fun: TransformerWithState[T]): TransformerWithState[T] = {
    lazy val local_transformer: TransformerWithState[T] = (state: T) => fun(state) orElse traverse(state)
    def transform_traverse(state: T): Transformer =
      local_transformer(state) orElse super.transformerWithState(local_transformer)(state)
    def tt[A <: Exp](state: T)(exp: A) = transform_traverse(state)(exp).asInstanceOf[A]
    def traverse(state: T): Transformer = {
      case cd: ColDef[_] => cd.copy(col = tt(state)(cd.col))
      case cd: ChildDef => cd.copy(exp = tt(state)(cd.exp))
      case fd: FunDef[_] => fd.copy(exp = tt(state)(fd.exp))
      case td: TableDef => td.copy(exp = tt(state)(td.exp))
      case to: TableObj => to.copy(obj = tt(state)(to.obj))
      case ta: TableAlias => ta.copy(obj = tt(state)(ta.obj))
      case sd: SelectDef =>
        val t = sd.tables map tt(state)
        val c = sd.cols map tt(state)
        val q = tt(state)(sd.exp)
        sd.copy(cols = c, tables = t, exp = q)
      case bd: BinSelectDef => bd.copy(
        leftOperand = tt(state)(bd.leftOperand),
        rightOperand = tt(state)(bd.rightOperand)
      )
      case id: InsertDef =>
        val t = id.tables map tt(state)
        val c = id.cols map tt(state)
        val i = tt(state)(id.exp)
        InsertDef(c, t, i)
      case ud: UpdateDef =>
        val t = ud.tables map tt(state)
        val c = ud.cols map tt(state)
        val u = tt(state)(ud.exp)
        UpdateDef(c, t, u)
      case dd: DeleteDef => DeleteDef(dd.tables map tt(state), tt(state)(dd.exp))
      case ad: ArrayDef => ad.copy(cols = ad.cols map tt(state))
      case rd: RecursiveDef => rd.copy(exp = tt(state)(rd.exp))
      case wtd: WithTableDef => wtd.copy(
        cols = wtd.cols map tt(state),
        tables = wtd.tables map tt(state),
        exp = tt(state)(wtd.exp)
      )
      case wsd: WithSelectDef => WithSelectDef(tt(state)(wsd.exp), wsd.withTables map tt(state))
      case wdml: WithDMLDef => WithDMLDef(tt(state)(wdml.exp), wdml.withTables map tt(state))
      case BracesSelectDef(sdb) => BracesSelectDef(tt(state)(sdb))
      case fsd: FunSelectDef => fsd.copy(exp = tt(state)(fsd.exp))
      case vfsd: ValuesFromSelectDef => vfsd.copy(exp = tt(state)(vfsd.exp))
    }
    transform_traverse
  }

  override def traverser[T](fun: Traverser[T]): Traverser[T] = {
    lazy val local_traverse: Traverser[T] = (state: T) => fun(state) orElse traverse(state)
    def fun_traverse(state: T) = local_traverse(state) orElse super.traverser(local_traverse)(state)
    //helper functions
    def tr(state: T, e: Exp): T = fun_traverse(state)(e)
    def trl(state: T, l: List[Exp]) = l.foldLeft(state) { (fr, el) => tr(fr, el) }
    def traverse(state: T): PartialFunction[Exp, T] = {
      case cd: ColDef[_] => tr(state, cd.col)
      case cd: ChildDef => tr(state, cd.exp)
      case fd: FunDef[_] => tr(state, fd.exp)
      case td: TableDef => tr(state, td.exp)
      case to: TableObj => tr(state, to.obj)
      case ta: TableAlias => tr(state, ta.obj)
      case bd: BinSelectDef => tr(tr(state, bd.leftOperand), bd.rightOperand)
      case ad: ArrayDef => trl(state, ad.cols)
      case rd: RecursiveDef => tr(state, rd.exp)
      case wtd: WithTableDef => tr(state, wtd.exp)
      case wq: WithQuery => tr(trl(state, wq.withTables), wq.exp)
      case bsd: BracesSelectDef => tr(state, bsd.exp)
      case fsd: FunSelectDef => tr(state, fsd.exp)
      case vfsd: ValuesFromSelectDef => tr(state, vfsd.exp)
      case sql: SQLDefBase => tr(trl(trl(state, sql.tables), sql.cols), sql.exp)
    }
    fun_traverse
  }

  def parseExp(expr: String): Exp = try {
    intermediateResults.get.clear
    phrase(exprList)(new scala.util.parsing.input.CharSequenceReader(expr)) match {
      case Success(r, _) => r
      case x => throw CompilerException(x.toString, x.next.pos)
    }
  } finally intermediateResults.get.clear
}
