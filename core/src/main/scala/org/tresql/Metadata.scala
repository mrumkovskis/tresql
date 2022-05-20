package org.tresql

import org.tresql.metadata.Procedure
import org.tresql.resources.{FunctionSignatures, FunctionSignaturesLoader, MacrosLoader}

import sys._

/** Implementation of meta data must be thread safe */
trait AbstractMetadata extends metadata.TypeMapper {
  import metadata._
  def join(table1: String, table2: String): (key_, key_) = {
    val t1 = table(table1); val t2 = table(table2)
    (t1.refs(t2.name), t2.refs(t1.name)) match {
      case (k1, k2) if k1.length + k2.length > 1 =>
        val r1 = reduceRefs(k1, t2.key)
        val r2 = reduceRefs(k2, t1.key)
        if (r1.length + r2.length == 1)
          if (r1.length == 1) (fk(r1.head.cols), uk(r1.head.refCols)) else (uk(r2.head.refCols), fk(r2.head.cols))
        else if (r1.length == 1) (fk(r1.head.cols), uk(r1.head.refCols))
        else error(s"Ambiguous relation. Too many found between tables $table1, $table2: ($r1, $r2)")
      case (k1, k2) if k1.length + k2.length == 0 => { //try to find two imported keys of the same primary key
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
  def procedure(name: String): Procedure[_] = ???
  def procedureOption(name: String): Option[Procedure[_]] = ???
}

trait Metadata extends AbstractMetadata {
  override def procedure(name: String): Procedure[_] = procedureOption(name)
    .getOrElse(sys.error(s"Function not found: $name"))
  override def procedureOption(name: String): Option[Procedure[_]] = {
    val idx = name.lastIndexOf("#")
    val pname = name.substring(0, idx)
    val pcount = name.substring(idx + 1, name.length).toInt
    functionSignatures.signatures.get(pname)
      .flatMap {
        case List(p) => Option(p)
        case p @ List(_*) =>
          //return procedure where parameter count match
          p.find(_.pars.size == pcount)
            .orElse(p.find(_.pars.size - 1 == pcount))
            .orElse(Option(p.maxBy(_.pars.size)))
      }
  }

  /** Override this to load function signatures from other resource than tresql-function-signatures.txt file */
  def functionSignaturesResource: String = null
  /** Override this to load function signatures from other resource than tresql-macros.txt file */
  def macroSignaturesResource: String = null
  /** Override this to load function signatures from object with tresql macros implementations */
  def macroClass: Class[_] = null

  private val functionSignatures: FunctionSignatures = {
    val sl = new FunctionSignaturesLoader(this)
    val ml = new MacrosLoader(this)
    def loadFromLoader(l: FunctionSignaturesLoader, res: String) = {
      if (res == null) l.loadFunctionSignatures(l.load())
      else l.load(res).map(sl.loadFunctionSignatures).getOrElse(FunctionSignatures.empty)
    }
    val fromSignResources   = loadFromLoader(sl, functionSignaturesResource)
    val fromMacroResources  = loadFromLoader(ml, macroSignaturesResource)
    val fromClass           = sl.loadFunctionSignaturesFromClass(macroClass)
    fromClass.merge(fromMacroResources.merge(fromSignResources))
  }
}

//TODO pk col storing together with ref col (for multi col key secure support)?
package metadata {
  case class Table(name: String, cols: List[Col[_]], key: Key,
      rfs: Map[String, List[Ref]]) {
    private val colMap: Map[String, Col[_]] = cols map (c => c.name.toLowerCase -> c) toMap
    val refTable: Map[List[String], String] = rfs.flatMap(t => t._2.map(_.cols -> t._1))
    def col(name: String) = colMap(name.toLowerCase)
    def colOption(name: String) = colMap.get(name.toLowerCase)
    def refs(table: String) = rfs.getOrElse(table, Nil)
    def ref(table: String, ref: List[String]) = rfs(table)
    .find(_.cols == ref)
    .getOrElse(error(s"""Column(s) "$ref" is not reference from table "$name" to table "$table" """))
  }
  object Table {
    def apply(t: Map[String, Any]): Table = {
      Table(t("name").toString.toLowerCase, t("cols") match {
        case l: List[Map[String, String] @unchecked] => l map { c =>
          Col(
            c("name").toLowerCase,
            c("nullable").asInstanceOf[Boolean],
            c("sql-type").asInstanceOf[Int],
            c("scala-type").asInstanceOf[Manifest[_]]
          )
        }
      }, t("key") match { case l: List[String @unchecked] => Key(l map (_.toLowerCase)) }, t("refs") match {
        case l: List[Map[String, Any] @unchecked] => (l map { r =>
          (r("table").asInstanceOf[String].toLowerCase,
            r("refs") match {
              case l: List[List[(String, String)] @unchecked] => l map (rc => {
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
                          pars: List[Par[_]], returnSqlType: Int, returnTypeName: String, returnType: ReturnType,
                          hasRepeatedPar: Boolean = false) {
    def scalaReturnType: Manifest[T] = returnType match {
      case r: FixedReturnType[T@unchecked] => r.mf
      case ParameterReturnType(idx) => pars(idx).scalaType.asInstanceOf[Manifest[T]]
    }
  }
  case class Par[T](name: String, comments: String, parType: Int, sqlType: Int, typeName: String,
    scalaType: Manifest[T])
  sealed trait ReturnType
  case class ParameterReturnType(idx: Int) extends ReturnType
  case class FixedReturnType[T](mf: Manifest[T]) extends ReturnType
  sealed trait key_ { def cols: List[String] }
  case class uk(cols: List[String]) extends key_
  case class fk(cols: List[String]) extends key_
}
