package org.tresql

import java.util.NoSuchElementException
import sys._

/** Implementation of meta data must be thread safe */
trait MetaData {
  import metadata._
  def join(table1: String, table2: String) = {
    val t1 = table(table1); val t2 = table(table2)
    (t1.refs(t2.name), t2.refs(t1.name)) match {
      case (k1, k2) if (k1.length + k2.length > 1) =>
        if (k1.length > 1 || k2.length > 1)
          error("Ambiguous relation. Too many found between tables " + table1 + ", " + table2)
        else //take foreign key from the left table and primary key from the right table
          (k1.head.cols, t2.key.cols)
      case (k1, k2) if (k1.length + k2.length == 0) => { //try to find two imported keys of the same primary key
        t1.rfs.filter(_._2.size == 1).foldLeft(List[(List[String], List[String])]()) {
          (res, t1refs) =>
              t2.rfs.foldLeft(res)((r, t2refs) => if (t2refs._2.size == 1 && t1refs._1 == t2refs._1)
                (t1refs._2.head.cols -> t2refs._2.head.cols) :: r else r)
        } match {
          case Nil => error("Relation not found between tables " + table1 + ", " + table2)
          case List(r) => r
          case b => error("Ambiguous relation. Too many found between tables " + table1 + ", " +
            table2 + ". Relation columns: " + b)
        }
      }
      case (k1, k2) => if (k1.length == 1) (k1.head.cols, t2.key.cols) else (t1.key.cols, k2.head.cols)
    }
  }

  def col(table: String, col: String): Col = this.table(table).col(col)
  def colOption(table: String, col: String): Option[Col] = this.tableOption(table).flatMap(_.colOption(col))
  def col(col: String): Col =
    table(col.substring(0, col.lastIndexOf('.'))).col(col.substring(col.lastIndexOf('.') + 1))
  def colOption(col: String): Option[Col] = tableOption(col.substring(0, col.lastIndexOf('.'))).flatMap(
    _.colOption(col.substring(col.lastIndexOf('.') + 1)))

  def table(name: String): Table
  def tableOption(name: String): Option[Table]
  def procedure(name: String): Procedure
  def procedureOption(name: String): Option[Procedure]
}

//TODO pk col storing together with ref col (for multi col key secure support)?
package metadata {
  case class Table(name: String, comments: String, cols: List[Col], key: Key,
      rfs: Map[String, List[Ref]], dbname: String) {
    private val colMap = cols map (c => c.name -> c) toMap
    val refTable: Map[Ref, String] = rfs.flatMap(t => t._2.map(_ -> t._1))
    def col(name: String) = colMap(name)
    def colOption(name: String) = colMap.get(name)
    def refs(table: String) = rfs.get(table).getOrElse(Nil)
  }
  object Table {
    def apply(t: Map[String, Any]): Table = {
      Table(t("name").toString.toLowerCase, t("comments").asInstanceOf[String], t("cols") match {
        case l: List[Map[String, String]] => l map { c =>
          Col(c("name").toString.toLowerCase, c("sqlType").asInstanceOf[Int],
            c("typeName").toString.toLowerCase, c("nullable").asInstanceOf[Boolean],
            c("size").asInstanceOf[Int], c("decimalDigits").asInstanceOf[Int],
            c("comments").asInstanceOf[String])
        }
      }, t("key") match { case l: List[String] => Key(l map (_.toLowerCase)) }, t("refs") match {
        case l: List[Map[String, Any]] => (l map { r =>
          (r("table").asInstanceOf[String].toLowerCase,
            r("refs") match {
              case l: List[List[String]] => l map (rc => Ref(rc map (_.toLowerCase)))
            })
        }).toMap
      }, if (t.get("dbname") == None) null else t("dbname").asInstanceOf[String])
    }
  }
  case class Col(name: String, sqlType: Int, typeName: String, nullable: Boolean,
    size: Int, decimalDigits: Int, comments: String)
  case class Key(cols: List[String])
  case class Ref(cols: List[String])
  case class Procedure(name: String, comments: String, procType: Int,
    pars: List[Par], returnSqlType: Int, returnTypeName: String)
  case class Par(name: String, comments: String, parType: Int, sqlType: Int, typeName: String)
}