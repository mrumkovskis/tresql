package org.tresql

import scala.util.Try
import sys._

trait Typer { this: QueryBuilder =>

  trait Def { def alias: String }
  case class TableDef(name: String, alias: String) extends Def
  case class SelectDef(tables: List[Def], alias: String) extends Def

  /* Finds top level alias. */
  def findAliasByName(name: String) = {
    def exists(d: Def): Boolean = d match {
      case TableDef(n, _) => n == name
      case SelectDef(t, _) => t exists exists
    }
    tableDefs find exists map (_.alias)
  }

  def findJoin(table: String) = {
    def findNested(nestedDefs: List[Def]): Option[(List[String], List[String])] = nestedDefs match {
      case Nil => None
      case d :: l => findDef(d) orElse findNested(l)
    }
    def findDef(d: Def) = d match {
      case TableDef(t, _) => Try(env.join(table, t)).toOption
      case SelectDef(ts, _) => findNested(ts)
    }
    def find(defs: List[Def]): Option[((List[String], List[String]), String)] = defs match {
      case Nil => error("Unable to find relationship between table " + table +
        " and parent query (tables, aliases) - " + tableDefs)
      case d :: l => findDef(d).map(_ -> d.alias).orElse(find(l))
    }
    find(tableDefs)
  }

  def head = {
    def h(d: Def): Def = d match { case t: TableDef => t case s: SelectDef => h(s.tables.head) }
    h(tableDefs.head)
  }
  
  def last = {
    def l(d: Def): Def = d match { case t: TableDef => t case s: SelectDef => l(s.tables.last) }
    l(tableDefs.last)    
  }

  def defs(tables: List[Table]) = {
    def extractDef(table: Table): Def = table match {
      case Table(IdentExpr(n), a, null | TableJoin(_, _, false, _), _, _) =>
        val name = n mkString "."
        TableDef(name, if (a == null) name else a)
      case Table(sel: SelectExpr, a, null | TableJoin(_, _, false, _), _, _) if a != null =>
        SelectDef(sel.tables map extractDef filter (_ != null), a)
      case Table(BracesExpr(sel: SelectExpr), a, null | TableJoin(_, _, false, _), _, _) if a != null =>
        SelectDef(sel.tables map extractDef filter (_ != null), a)
      //extract def from deep braces
      case t @ Table(b: BracesExpr, _, _, _, _) => extractDef(t.copy(table = b.expr))
      case _ => null
    }
    tables map extractDef filter (_ != null)
  }

}
