package org.tresql.compiling

import org.tresql.parsing._
import org.tresql.metadata._
import org.tresql.{Env, Result, RowLike}
import scala.reflect.ManifestFactory

trait Scope {
  def parent: Scope
  def table(table: String): Option[Table]
  def column(col: String): Option[Col[_]]
  def procedure(procedure: String): Option[Procedure]
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

  case class TableDef(name: String, exp: Exp)

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
  def procedure(procedure: String) = None
  def parent = null

  def buildTypedDef(exp: Exp) = {
    val typedExp = transform(exp, {
      case f: Fun => f
      case c: Col => c
      case o: Obj => o
      case q: Query => q
      case b: BinOp => b
      case c @ UnOp("|", _) => c
    })
  }
}
