package org.tresql

import java.util.NoSuchElementException
import sys._

//TODO consider letting metadata to span multiple databases?
//Implementation of meta data must be thread safe
trait MetaData {
  import metadata._
  def join(table1: String, table2: String) = {
    val t1 = table(table1); val t2 = table(table2)
    (t1.refs(t2.name), t2.refs(t1.name)) match {
      case (k1, k2) if (k1.length + k2.length > 1) => error("Ambiguous relation. Too many found between tables " +
        table1 + ", " + table2)
      case (k1, k2) if (k1.length + k2.length == 0) => error("Relation not found between tables " + table1 + ", " + table2)
      case (k1, k2) => if (k1.length == 1) (k1(0).cols, t2.key.cols) else (t1.key.cols, k2(0).cols)
    }
  }

  def col(table: String, col: String):Col = this.table(table).cols(col)
  def colOption(table:String, col:String):Option[Col] = this.tableOption(table).flatMap(_.cols.get(col))
  def col(col: String):Col = 
    table(col.substring(0, col.lastIndexOf('.'))).cols(col.substring(col.lastIndexOf('.') + 1))
  def colOption(col: String):Option[Col] = tableOption(col.substring(0, col.lastIndexOf('.'))).flatMap(
      _.cols.get(col.substring(col.lastIndexOf('.') + 1)))

  def dbName: String
  def table(name: String): Table
  def tableOption(name: String): Option[Table]
  def procedure(name: String): Procedure
  def procedureOption(name: String): Option[Procedure]
}

//TODO pk col storing together with ref col (for multi col key secure support)?
package metadata {
  case class Table(val name:String, val comments: String, val cols: Map[String, Col], val key: Key,
    rfs: Map[String, List[Ref]], dbname: String) {
    val refTable:Map[Ref, String] = rfs.flatMap(t=> t._2.map(_ -> t._1))
    def refs(table: String) = rfs.get(table).getOrElse(Nil)
  }
  object Table {
    def apply(t: Map[String, Any]): Table = {
      Table(t("name").toString.toLowerCase, t("comments").asInstanceOf[String], t("cols") match {
        case l:List[Map[String, String]] => (l map {c => (c("name").toLowerCase,
            Col(c("name").toString.toLowerCase, c("sqlType").asInstanceOf[Int],
                c("typeName").toString.toLowerCase, c("nullable").asInstanceOf[Boolean],
                c("size").asInstanceOf[Int], c("decimalDigits").asInstanceOf[Int],
                c("comments").asInstanceOf[String]))}).toMap
      }, t("key") match { case l:List[String] => Key(l map (_.toLowerCase)) }, t("refs") match {
        case l:List[Map[String, Any]] => (l map {r => (r("table").asInstanceOf[String].toLowerCase,
            r("refs") match {
              case l:List[List[String]] => l map (rc => Ref(rc map (_.toLowerCase)))
        })}).toMap }, if (t.get("dbname") == None) null else t("dbname").asInstanceOf[String]
      )
    }
  }
  case class Col(val name: String, val sqlType: Int, val typeName: String, val nullable: Boolean,
      size: Int, decimalDigits: Int, comments: String)
  case class Key(val cols: List[String])
  case class Ref(val cols: List[String])
  case class Procedure(val name: String, val comments: String, val procType: Int,
    val pars: List[Par], val returnSqlType: Int, val returnTypeName: String)
  case class Par(val name: String, val comments: String, val parType: Int, val sqlType: Int,
      val typeName: String)
}