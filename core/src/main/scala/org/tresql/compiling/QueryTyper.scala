package org.tresql.compiling

import org.tresql.parsing._
import org.tresql.metadata._

trait Scope {
  def parent: Scope
  def table(table: String): Option[Table]
  def column(col: String): Option[Col]
}

trait QueryTyper extends QueryParsers with ExpTransformer with Scope {
  trait Def {
    def exp: Exp
    def name: String
  }

  case class ColumnDef[T](name: String, exp: Exp)(implicit typ: Manifest[T]) extends Def

  case class TableDef(exp: Ident) extends Def {
    def name = exp.ident mkString "."
  }
  case class SelectDef(
    name: String,
    cols: List[ColumnDef[_]],
    exp: Exp,
    parent: Scope) extends Scope with Def {

    def check = true
    def table(table: String) = None
    def column(col: String) = None

  }
  case class FunDef[T](name: String, exp: Exp)(typ: Manifest[T]) extends Def


  def table(table: String) = None
  def column(col: String) = None

}
