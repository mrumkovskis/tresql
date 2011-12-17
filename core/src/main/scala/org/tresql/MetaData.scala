package org.tresql

import java.util.NoSuchElementException
import sys._

//TODO consider letting metadata to span multiple databases?
//Implementation of meta data must be thread safe
trait MetaData {
  import metadata._
  protected implicit def conn:java.sql.Connection = null
  def join(table1: String, table2: String) = {
    val t1 = table(table1); val t2 = table(table2)
    (t1.refs(t2.name), t2.refs(t1.name)) match {
      case (k1, k2) if (k1.length + k2.length > 1) => error("Ambiguous relation. Too many found between tables " +
        table1 + ", " + table2)
      case (k1, k2) if (k1.length + k2.length == 0) => error("Relation not found between tables " + table1 + ", " + table2)
      case (k1, k2) => if (k1.length == 1) (k1(0).cols, t2.key.cols) else (t1.key.cols, k2(0).cols)
    }
  }

  def col(table: String, col: String) = this.table(table).cols(col)
  def col(col: String) = table(col.substring(0, col.lastIndexOf('.'))).cols(col.substring(col.lastIndexOf('.') + 1))

  def dbName: String
  //TODO rename tbl->table, table->tableData or something
  def tbl(name: String) = table(name)
  def table(name: String)(implicit conn: java.sql.Connection): Table

}

//TODO db names separation from business names, column types, sizes, nullability etc...
//TODO pk col storing together with ref col (for multi col key secure support)?
package metadata {
  case class Table(val name:String, val cols: Map[String, Col], val key: Key, private val rfs: Map[String, List[Ref]]) {
    def refs(table: String) = try { rfs(table) } catch { case _:NoSuchElementException => Nil }
  }
  object Table extends ((Map[String, Any]) => Table) {
    def apply(t: Map[String, Any]): Table = {
      Table(t("name").asInstanceOf[String], t("cols") match {
        case l:List[Map[String, String]] => (l map {c => (c("name"), Col(c("name"), c("type")))}).toMap
      }, t("key") match { case l:List[String] => Key(l) }, t("refs") match {
        case l:List[Map[String, Any]] => (l map {r => (r("table").asInstanceOf[String], r("refs") match {
          case l:List[List[String]] => l map (Ref(_))
        })}).toMap }
      )
    }
  }
  case class Col(val name: String, val colType: String)
  case class Key(val cols: List[String])
  case class Ref(val cols: List[String])

}