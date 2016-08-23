package org.tresql.compiling

import org.tresql.parsing._
import org.tresql.metadata._
import org.tresql.{Env, Result, RowLike}
import scala.reflect.ManifestFactory

trait Scope {
  def parent: Scope
  def table(table: String): Option[Table]
  def column(col: String): Option[Col]
  def procedure(procedure: String): Option[Procedure]
}
trait CompiledResult[T <: RowLike] extends Result with Iterator[T] {
  override def toList: List[T] = Nil
}

trait QueryTyper extends QueryParsers with ExpTransformer with Scope {
  val metadata = Env.metaData
  trait Def[T] {
    def exp: Exp
    def name: String
    def typ: Manifest[T]
  }
  case class ColumnDef[T](name: String, exp: Exp)(implicit val typ: Manifest[T]) extends Def[T]

  case class TableDef(exp: Ident) extends Def[TableDef] {
    val typ: Manifest[TableDef] = ManifestFactory.classType(this.getClass)
    def name = exp.ident mkString "."
  }
  case class SelectDef(
    name: String,
    cols: List[ColumnDef[_]],
    tables: List[Def[_]],
    exp: Exp,
    parent: Scope) extends Scope with Def[SelectDef] {

    val typ: Manifest[SelectDef] = ManifestFactory.classType(this.getClass)
    def check = true
    def table(table: String) = None
    def column(col: String) = None
    def procedure(procedure: String) = None
  }
  case class FunDef[T](name: String, exp: Exp)(implicit val typ: Manifest[T]) extends Def[T]

  def table(table: String) = metadata.tableOption(table)
  def column(col: String) = metadata.colOption(col)
  def procedure(procedure: String) = None
}
