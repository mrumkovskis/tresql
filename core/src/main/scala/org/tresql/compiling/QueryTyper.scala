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

  trait ColumnDefBase[T] extends TypedExp[T] {
    def name: String
  }
  case class ColumnDef[T](name: String, exp: Exp)(implicit val typ: Manifest[T]) extends ColumnDefBase[T]
  case class ChildSelectDef(name: String, exp: SelectDefBase) extends ColumnDefBase[ChildSelectDef] {
    val typ: Manifest[ChildSelectDef] = ManifestFactory.classType(this.getClass)
  }
  case class FunDef[T](name: String, exp: Fun)(implicit val typ: Manifest[T]) extends TypedExp[T]

  case class TableDef(name: String, exp: Exp) extends Exp { def tresql = exp.tresql }

  trait SelectDefBase extends Scope with TypedExp[SelectDefBase] {
    def cols: List[ColumnDefBase[_]]
    val typ: Manifest[SelectDefBase] = ManifestFactory.classType(this.getClass)
  }
  case class SelectDef(
    cols: List[ColumnDefBase[_]],
    tables: List[TableDef],
    exp: Exp,
    parent: Scope) extends SelectDefBase {

    def table(table: String) = None
    def column(col: String) = None
    def procedure(procedure: String) = None
  }
  case class BinSelectDef(
    leftOperand: SelectDefBase,
    rightOperand: SelectDefBase,
    exp: Exp,
    parent: Scope) extends SelectDefBase {

    val cols = Nil
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

    def tr(x: Any): Any = x match {case e: Exp => builder(e) case _ => x}
    lazy val builder: PartialFunction[Exp, Exp] = {
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
      case o: Obj if ctx.head == QueryCtx => builder(Query(List(o), null, null, false, null, null, null, null))
      case o: Obj if ctx.head == TablesCtx =>
        val no = o.copy(obj = builder(o.obj), join = builder(o.join).asInstanceOf[Join])
        if (o.alias != null) TableDef(o.alias, no)
        else o match {
          case Obj(Ident(name), _, _, _, _) => TableDef(name mkString ".", no)
          case _ => sys.error(s"Alias missing for from clause select: ${o.tresql}")
        }
      case o: Obj if ctx.head == BodyCtx => o.copy(obj = builder(o.obj), join = builder(o.join).asInstanceOf[Join])
      case q: Query => q
      case b: BinOp => b
      case c @ UnOp("|", _) if ctx.head == ColsCtx => c
    }
    val typedExp = transform(exp, builder)
  }
}
