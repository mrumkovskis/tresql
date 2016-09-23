package org.tresql.compiling

import org.tresql.parsing._
import org.tresql.metadata._
import org.tresql.{Env, Result, RowLike}
import scala.reflect.ManifestFactory

trait Scope {
  def parent: Scope
  def table(table: String): Option[Table]
  def column(col: String): Option[Col[_]]
  def procedure(procedure: String): Option[Procedure[_]]
}
trait CompiledResult[T <: RowLike] extends Result with Iterator[T] {
  override def toList: List[T] = Nil
}

trait QueryTyper extends QueryParsers with ExpTransformer with Scope { thisTyper =>

  var nameIdx = 0
  val metadata = Env.metaData

  trait TypedExp[T] extends Exp {
    def exp: Exp
    def typ: Manifest[T]
    def tresql = exp.tresql
  }

  case class ColumnDef[T](name: String, exp: Col)(implicit val typ: Manifest[T]) extends TypedExp[T]
  case class ChildDef(exp: Exp) extends TypedExp[ChildDef] {
    val typ: Manifest[ChildDef] = ManifestFactory.classType(this.getClass)
  }
  case class FunDef[T](name: String, exp: Fun)(implicit val typ: Manifest[T]) extends TypedExp[T]

  case class TableDef(name: String, exp: Exp) extends Exp { def tresql = exp.tresql }

  trait SelectDefBase extends Scope with TypedExp[SelectDefBase] {
    def cols: List[ColumnDef[_]]
    val typ: Manifest[SelectDefBase] = ManifestFactory.classType(this.getClass)
  }
  case class SelectDef(
    cols: List[ColumnDef[_]],
    tables: List[TableDef],
    exp: Query,
    parent: Scope) extends SelectDefBase {

    //check for duplicating tables
    {
      val duplicates = tables.groupBy(_.name).filter(_._2.size > 1).map(_._1)
      assert(duplicates.size == 0, s"Duplicate table names: ${duplicates.mkString(", ")}")
    }

    def table(table: String) = tables.find(_.name == table).flatMap {
      case TableDef(_, Obj(Ident(name), _, _, _, _)) => parent.table(name mkString ".")
      case TableDef(_, s: SelectDefBase) => null //todo return table based on columns
    } orElse parent.table(table)
    def column(col: String) = None
    def procedure(procedure: String) = parent.procedure(procedure)
  }
  case class BinSelectDef(
    leftOperand: SelectDefBase,
    rightOperand: SelectDefBase,
    exp: BinOp,
    parent: Scope) extends SelectDefBase {

    assert (leftOperand.cols.size != rightOperand.cols.size,
      s"Column count do not match ${leftOperand.cols.size} != ${rightOperand.cols.size}!")
    val cols = leftOperand.cols
    def table(table: String) = None
    def column(col: String) = None
    def procedure(procedure: String) = None
  }

  def table(table: String) = metadata.tableOption(table)
  def column(col: String) = metadata.colOption(col)
  def procedure(procedure: String) = metadata.procedureOption(procedure)
  def parent = null


  def buildTypedDef(exp: Exp) = {
    trait Ctx
    object QueryCtx extends Ctx //root context
    object TablesCtx extends Ctx //from clause
    object ColsCtx extends Ctx //column clause
    object BodyCtx extends Ctx //where, group by, having, order, limit clauses
    val ctx = scala.collection.mutable.Stack[Ctx](QueryCtx)

    def tr(x: Any): Any = x match {case e: Exp => builder(e) case _ => x} //helper function
    lazy val builder: PartialFunction[Exp, Exp] = transformFunction {
      case f: Fun => procedure(f.name).map { p =>
        FunDef(p.name, f.copy(parameters = f.parameters map tr))(p.scalaReturnType)
      }.getOrElse(sys.error(s"Unknown function: ${f.name}"))
      case c: Col =>
        val alias = if (c.alias != null) c.alias else c.col match {
          case Obj(Ident(name), _, _, _, _) => name mkString "."
          case _ => null
        }
        ColumnDef(alias, c.copy(col = tr(c.col)))(
          if(c.typ != null) metadata.xsd_scala_type_map(c.typ) else ManifestFactory.Nothing)
      case Obj(b: Braces, _, _, _, _) if ctx.head == QueryCtx =>
        builder(b) //unwrap braces top level expression
      case o: Obj if ctx.head == QueryCtx | ctx.head == TablesCtx => //obj as query
        builder(Query(List(o), null, null, false, null, null, null, null))
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
          TableDef(name, table.copy(obj = newTable, join = join))
        }
        ctx.pop
        ctx push ColsCtx
        val cols = if (q.cols != null) (q.cols map builder).asInstanceOf[List[ColumnDef[_]]] else null
        ctx.pop
        ctx push BodyCtx
        val (filter, grp, ord, limit, offset) =
          (tr(q.filter).asInstanceOf[Filters],
           tr(q.group).asInstanceOf[Grp],
           tr(q.order).asInstanceOf[Ord],
           tr(q.limit),
           tr(q.offset))
        ctx.pop
        SelectDef(cols, tables,
          q.copy(filter = filter, group = grp, order = ord, limit = limit, offset = offset),
          null)
      case b: BinOp =>
        (tr(b.lop), tr(b.rop)) match {
          case (lop: SelectDefBase, rop: SelectDefBase) =>
            BinSelectDef(lop, rop, b.copy(lop = lop, rop = rop), null)
          case (lop, rop) => b.copy(lop = lop, rop = rop)
        }
      case UnOp("|", o: Exp) if ctx.head == ColsCtx => ChildDef(builder(o))
    }
    builder(exp)
  }

  def parseExp(expr: String): Any = try {
    intermediateResults.get.clear
    phrase(exprList)(new scala.util.parsing.input.CharSequenceReader(expr)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }
  } finally intermediateResults.get.clear


}
