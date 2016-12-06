package org.tresql

import java.util.NoSuchElementException
import sys._
import scala.reflect.Manifest

/** Implementation of meta data must be thread safe */
trait MetaData extends metadata.TypeMapper {
  import metadata._
  def join(table1: String, table2: String): (key_, key_) = {
    val t1 = table(table1); val t2 = table(table2)
    (t1.refs(t2.name), t2.refs(t1.name)) match {
      case (k1, k2) if (k1.length + k2.length > 1) =>
        val r1 = reduceRefs(k1, t2.key)
        val r2 = reduceRefs(k2, t1.key)
        if (r1.length + r2.length == 1)
          if (r1.length == 1) (fk(r1.head.cols), uk(r1.head.refCols)) else (uk(r2.head.refCols), fk(r2.head.cols))
        else if (r1.length == 1) (fk(r1.head.cols), uk(r1.head.refCols))
        else error(s"Ambiguous relation. Too many found between tables $table1, $table2: ($r1, $r2)")
      case (k1, k2) if (k1.length + k2.length == 0) => { //try to find two imported keys of the same primary key
        t1.rfs.filter(_._2.size == 1).foldLeft(List[(List[String], List[String])]()) {
          (res, t1refs) =>
              t2.rfs.foldLeft(res)((r, t2refs) => if (t2refs._2.size == 1 && t1refs._1 == t2refs._1)
                (t1refs._2.head.cols -> t2refs._2.head.cols) :: r else r)
        } match {
          case Nil => error("Relation not found between tables " + table1 + ", " + table2)
          case List((a, b)) => (fk(a), fk(b))
          case b => error(s"Ambiguous relation. Too many found between tables $table1, $table2. Relation columns: $b")
        }
      }
      case (k1, k2) =>
        if (k1.length == 1) (fk(k1.head.cols), uk(k1.head.refCols)) else (uk(k2.head.refCols), fk(k2.head.cols))
    }
  }

  private def reduceRefs(refs: List[Ref], key: Key) = {
    def importedPkKeyCols(ref: Ref, key: Key) = {
      val refsPk = ref.cols zip ref.refCols
      key.cols.foldLeft(Option(List[String]())) {
        (importedKeyCols, keyCol) =>
          importedKeyCols.flatMap(l => refsPk.find(_._2 == keyCol).map(_._1 :: l))
      } map (_.reverse)
    }

    refs.groupBy(importedPkKeyCols(_, key)) match {
      case m if m.size == 1 && m.head._1.isDefined => List(refs.minBy(_.cols.size))
      case _ => refs
    }
  }

  def col(table: String, col: String): Col[_] = this.table(table).col(col)
  def colOption(table: String, col: String): Option[Col[_]] = this.tableOption(table).flatMap(_.colOption(col))
  def col(col: String): Col[_] = colOption(col)
    .getOrElse(sys.error(s"Column not found: $col"))
  def colOption(col: String): Option[Col[_]] = {
    val colIdx = col.lastIndexOf('.')
    if (colIdx == -1) sys.error(s"Table must be specified for col in format: <table>.$col")
    tableOption(col.substring(0, colIdx)).flatMap(_.colOption(col.substring(colIdx + 1)))
  }

  def table(name: String): Table
  def tableOption(name: String): Option[Table]
  /*??? implementations so super can be called from extending trait in the case
  two implementations of this trait are mixed together.
  This pattern is used in {{{compiling.CompilerFunctions}}} */
  def procedure(name: String): Procedure[_] = ???
  def procedureOption(name: String): Option[Procedure[_]] = ???
}

//TODO pk col storing together with ref col (for multi col key secure support)?
package metadata {
  case class Table(name: String, cols: List[Col[_]], key: Key,
      rfs: Map[String, List[Ref]]) {
    private val colMap: Map[String, Col[_]] = cols map (c => c.name -> c) toMap
    val refTable: Map[List[String], String] = rfs.flatMap(t => t._2.map(_.cols -> t._1))
    def col(name: String) = colMap(name)
    def colOption(name: String) = colMap.get(name)
    def refs(table: String) = rfs.get(table).getOrElse(Nil)
    def ref(table: String, ref: List[String]) = rfs(table)
    .find(_.cols == ref)
    .getOrElse(error(s"""Column(s) "$ref" is not reference from table "$name" to table "$table" """))
  }
  object Table {
    def apply(t: Map[String, Any]): Table = {
      Table(t("name").toString.toLowerCase, t("cols") match {
        case l: List[Map[String, String]] => l map { c =>
          Col(
            c("name").toString.toLowerCase,
            c("nullable").asInstanceOf[Boolean],
            c("sql-type").asInstanceOf[Int],
            c("scala-type").asInstanceOf[Manifest[_]]
          )
        }
      }, t("key") match { case l: List[String] => Key(l map (_.toLowerCase)) }, t("refs") match {
        case l: List[Map[String, Any]] => (l map { r =>
          (r("table").asInstanceOf[String].toLowerCase,
            r("refs") match {
              case l: List[List[(String, String)]] => l map (rc => {
                  val t = rc.unzip
                  Ref(t._1 map (_.toLowerCase), t._2 map (_.toLowerCase))
              })
            })
        }).toMap
      })
    }
  }
  case class Col[T](name: String, nullable: Boolean, sqlType: Int, scalaType: Manifest[T])
  case class Key(cols: List[String])
  case class Ref(cols: List[String], refCols: List[String])
  case class Procedure[T](name: String, comments: String, procType: Int,
    pars: List[Par[_]], returnSqlType: Int, returnTypeName: String, scalaReturnType: Manifest[T],
    hasRepeatedPar: Boolean = false) {
    /**Returns parameter index return type depends on during runtime or -1 if return type does not
    depend on any parameter type. */
    def returnTypeParIndex: Int = if (scalaReturnType == null) -1 else {
      import java.lang.reflect._
      val c = scalaReturnType.runtimeClass
      if (classOf[TypeVariable[_ <: GenericDeclaration]].isAssignableFrom(c)) {
        //return type is parameterized
        pars.indexWhere(_.scalaType.toString == scalaReturnType.toString)
      } else -1
    }
  }
  case class Par[T](name: String, comments: String, parType: Int, sqlType: Int, typeName: String,
    scalaType: Manifest[T])
  trait key_ { def cols: List[String] }
  case class uk(cols: List[String]) extends key_
  case class fk(cols: List[String]) extends key_
}
