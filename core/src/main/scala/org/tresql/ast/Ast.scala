package org.tresql.ast

private[tresql] object QueryParsers {
  private[tresql] val simple_ident_regex = "^[_\\p{IsLatin}][_\\p{IsLatin}0-9]*$".r

  def any2tresql(any: Any): String = any match {
    case a: String => if (a.contains("'")) "\"" + a + "\"" else "'" + a + "'"
    case e: Exp @unchecked => e.tresql
    case null => "null"
    case l: List[_] => l map any2tresql mkString ", "
    case x => x.toString
  }
}

import QueryParsers.any2tresql
import org.tresql.metadata.Procedure

import scala.reflect.ManifestFactory

sealed trait Exp {
  def tresql: String
}

sealed trait DMLExp extends Exp {
  def table: Ident
  def alias: String
  def cols: List[Col]
  def filter: Arr
  def vals: Exp
  def returning: Option[Cols]
  def db: Option[String]
}

sealed trait Const extends Exp {
  def value: Any
  def tresql = value.toString
}
case class IntConst(value: Int) extends Const
case class StringConst(value: String) extends Const {
  override def tresql: String = if (value.contains("'")) "\"" + value + "\"" else "'" + value + "'"
}
case class BigDecimalConst(value: BigDecimal) extends Const
case class BooleanConst(value: Boolean) extends Const

case class Sql(sql: String) extends Exp {
  def tresql = s"`$sql`"
}
case class Ident(ident: List[String]) extends Exp {
  def tresql = ident.mkString(".")
}
case class Variable(variable: String, members: List[String] = Nil, opt: Boolean) extends Exp {
  def tresql = if (variable == "?") "?" else {
    def var_str(v: String) =
      (if (QueryParsers.simple_ident_regex.pattern.matcher(v).matches) v
      else if (v contains "'") "\"" + v + "\""
      else "'" + v + "'")
    ":" + var_str(variable) +
      (if (members == null | members == Nil) "" else "." + (members map var_str mkString ".")) +
      (if (opt) "?" else "")
  }
}
case class Id(name: String) extends Exp {
  def tresql = "#" + name
}
case class IdRef(name: String) extends Exp {
  def tresql = ":#" + name
}
case class Res(rNr: Int, col: Exp) extends Exp {
  def tresql = ":" + rNr + "(" + col.tresql + ")"
}
case class Cast(exp: Exp, typ: String) extends Exp {
  def tresql = exp.tresql + "::" +
    (if (QueryParsers.simple_ident_regex.pattern.matcher(typ).matches) typ else "'" + typ + "'")
}
case class UnOp(operation: String, operand: Exp) extends Exp {
  def tresql = operation + operand.tresql
}
case class ChildQuery(query: Exp, db: Option[String]) extends Exp {
  def tresql = "|" + db.map(_ + ":").mkString + query.tresql
}
case class Fun(
                name: String,
                parameters: List[Exp],
                distinct: Boolean,
                aggregateOrder: Option[Ord],
                aggregateWhere: Option[Exp]
              ) extends Exp {
  def tresql = name + "(" + (if (distinct) "# " else "") +
    ((parameters map any2tresql) mkString ", ") +
    s""")${aggregateOrder.map(o => " " + o.tresql).mkString}${
      aggregateWhere.map(e => s"[${e.tresql}]").mkString}"""
}

case class TableColDef(name: String, typ: Option[String])
case class FunAsTable(fun: Fun, cols: Option[List[TableColDef]], withOrdinality: Boolean) extends Exp {
  def tresql = fun.tresql
}

case class In(lop: Exp, rop: List[Exp], not: Boolean) extends Exp {
  def tresql = any2tresql(lop) + (if (not) " !" else " ") + rop.map(any2tresql).mkString(
    "in(", ", ", ")")
}
case class BinOp(op: String, lop: Exp, rop: Exp) extends Exp {
  def tresql = lop.tresql + " " + op + " " + rop.tresql
}
case class TerOp(lop: Exp, op1: String, mop: Exp, op2: String, rop: Exp) extends Exp {
  def content = BinOp("&", BinOp(op1, lop, mop), BinOp(op2, mop, rop))
  def tresql = s"${any2tresql(lop)} $op1 ${any2tresql(mop)} $op2 ${any2tresql(rop)}"
}

case class Join(default: Boolean, expr: Exp = null, noJoin: Boolean) extends Exp {
  def tresql = this match {
    case Join(_, _, true) => ";"
    case Join(false, a: Arr, false) => a.tresql
    case Join(false, e: Exp, false) => "[" + e.tresql + "]"
    case Join(true, null, false) => "/"
    case Join(true, e: Exp, false) => "/[" + e.tresql + "]"
    case _ => sys.error("Unexpected Join case")
  }
}

case class Obj(obj: Exp, alias: String = null, join: Join = null, outerJoin: String = null, nullable: Boolean = false)
  extends Exp {
  def tresql = {
    (if (join != null) join.tresql else "") + (if (outerJoin == "r") "?" else "") +
      obj.tresql + (if (outerJoin == "l") "?" else if (outerJoin == "i") "!" else "") +
      (obj match {
        case FunAsTable(_, cols, ord) =>
          " " + alias +
            cols
              .map(_.map(c => c.name + c.typ.map("::" + _).getOrElse(""))
                .mkString(if (ord) "(# " else "(", ", ", ")"))
              .getOrElse("")
        case _ => if (alias == null) "" else " " + alias
      })
  }
}
case class Col(col: Exp, alias: String = null) extends Exp {
  def tresql = any2tresql(col) + (if (alias != null) " " + alias else "")
}
case class Cols(distinct: Boolean, cols: List[Col]) extends Exp {
  def tresql = (if (distinct) "#" else "") + cols.map(_.tresql).mkString("{", ",", "}")
}
case class Grp(cols: List[Exp], having: Exp = null) extends Exp {
  def tresql = "(" + cols.map(any2tresql).mkString(",") + ")" +
    (if (having != null) "^(" + any2tresql(having) + ")" else "")
}
case class OrdCol(nullsFirst: Exp = null, exp: Exp, nullsLast: Exp = null) extends Exp {
  def tresql =
    (if (nullsFirst == Null) "null " else "") + any2tresql(exp) + (if (nullsLast == Null) " null" else "")
}
case class Ord(cols: List[OrdCol]) extends Exp {
  def tresql = "#(" + cols.map(_.tresql).mkString(",") + ")"
}
case class Query(tables: List[Obj], filter: Filters, cols: Cols = null,
                 group: Grp = null, order: Ord = null, offset: Exp = null, limit: Exp = null) extends Exp {
  def tresql = tables.map(any2tresql).mkString +
    filter.tresql +
    (if (cols != null) cols.tresql else "") +
    (if (group != null) any2tresql(group) else "") +
    (if (order != null) order.tresql else "") +
    (if (limit != null)
      s"@(${if (offset != null) any2tresql(offset) + " " else ""}${any2tresql(limit)})"
    else if (offset != null) s"@(${any2tresql(offset)}, )"
    else "")
}

case class WithTable(name: String, cols: List[String], recursive: Boolean, table: Exp)
  extends Exp {
  def tresql = s"$name (${if (!recursive) "# " else ""}${cols mkString ", "}) { ${table.tresql} }"
}
case class With(tables: List[WithTable], query: Exp) extends Exp {
  def tresql = tables.map(_.tresql).mkString(", ") + " " + query.tresql
}
case class Values(values: List[Arr]) extends Exp {
  def tresql = values map (_.tresql) mkString ", "
}
case class Insert(table: Ident, alias: String = null, cols: List[Col], vals: Exp = null, returning: Option[Cols],
                  db: Option[String])
  extends DMLExp {
  override def filter = null
  def tresql = "+" + db.map(_ + ":").mkString + table.tresql +
    Option(alias).map(" " + _).getOrElse("") +
    (if (cols.nonEmpty) cols.map(_.tresql).mkString("{", ",", "}") else "") +
    (if (vals != null) any2tresql(vals) else "") +
    returning.map(_.tresql).getOrElse("")
}
case class Update(table: Ident, alias: String = null, filter: Arr = null, cols: List[Col], vals: Exp = null,
                  returning: Option[Cols], db: Option[String])
  extends DMLExp {
  def tresql = {
    val filterTresql = if (filter != null) filter.tresql else ""
    val colsTresql = if (cols.nonEmpty) cols.map(_.tresql).mkString("{", ",", "}") else ""
    val valsTresql = if (vals != null) any2tresql(vals) else ""
    "=" + db.map(_ + ":").mkString + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (vals match {
        case vfs: ValuesFromSelect => valsTresql + filterTresql + colsTresql
        case _ => filterTresql + colsTresql + valsTresql
      }) +
      returning.map(_.tresql).getOrElse("")
  }
}
case class ValuesFromSelect(select: Query) extends Exp {
  def tresql =
    if (select.tables.size > 1) select.copy(tables = select.tables.tail).tresql
    else ""
}
case class Delete(table: Ident, alias: String = null, filter: Arr, using: Exp = null, returning: Option[Cols],
                  db: Option[String])
  extends DMLExp {
  override def cols = null
  override def vals = using
  def tresql = {
    val tbl =
      db.map(_ + ":").mkString + table.tresql + Option(alias).map(" " + _).getOrElse("") +
        (if (using == null) "" else using.tresql)

    "-" + tbl + filter.tresql + returning.map(_.tresql).getOrElse("")
  }
}
case class Arr(elements: List[Exp]) extends Exp {
  def tresql = "[" + any2tresql(elements) + "]"
}
case class Filters(filters: List[Arr]) extends Exp {
  def tresql = filters map any2tresql mkString
}
object All extends Exp {
  def tresql = "*"
}
case class IdentAll(ident: Ident) extends Exp {
  def tresql = ident.ident.mkString(".") + ".*"
}

sealed trait Null extends Exp {
  def tresql = "null"
}
case object Null extends Null
case object NullUpdate extends Null

case class Braces(expr: Exp) extends Exp {
  def tresql = "(" + any2tresql(expr) + ")"
}

class CompilerException(message: String,
                        val pos: scala.util.parsing.input.Position = scala.util.parsing.input.NoPosition,
                        cause: Exception = null
                       ) extends Exception(message, cause)

object CompilerAst {

  protected def error(msg: String, cause: Exception = null) = throw new CompilerException(msg, cause = cause)

  sealed trait CompilerExp extends Exp

  case class ExprType(name: String = null) {
    override def toString = name
  }

  object ExprType {
    val Any     = ExprType("Any")
    val Boolean = ExprType("Boolean")
    val Nothing = ExprType()
  }

  trait TypedExp extends CompilerExp {
    def exp: Exp

    def typ: ExprType

    def tresql: String = exp.tresql
  }

  case class TableDef(name: String, exp: Obj) extends CompilerExp {
    def tresql: String = exp.tresql
  }

  /** helper class for namer to distinguish table references from column references */
  case class TableObj(obj: Exp) extends CompilerExp {
    def tresql: String = obj.tresql
  }

  /** helper class for namer to distinguish table with NoJoin, i.e. must be defined in tables clause earlier */
  case class TableAlias(obj: Exp) extends CompilerExp {
    def tresql: String = obj.tresql
  }

  case class ColDef(name: String, col: Exp, typ: ExprType) extends TypedExp {
    def exp: ColDef = this

    override def tresql: String = Col(col, name).tresql
  }

  case class ChildDef(exp: Exp, db: Option[String]) extends TypedExp {
    val typ: ExprType = ExprType(this.getClass.getName)
  }

  case class FunDef(name: String, exp: Fun, typ: ExprType, procedure: Procedure)
    extends TypedExp {
    if ((procedure.hasRepeatedPar && exp.parameters.size < procedure.pars.size - 1) ||
      (!procedure.hasRepeatedPar && exp.parameters.size != procedure.pars.size))
      error(
        s"Function '$name' has wrong number of parameters: ${exp.parameters.size} instead of ${procedure.pars.size}")
  }

  case class FunAsTableDef(exp: FunDef, cols: Option[List[TableColDef]], withOrdinality: Boolean)
    extends CompilerExp {
    def tresql: String = FunAsTable(exp.exp, cols, withOrdinality).tresql
  }

  case class RecursiveDef(exp: Exp) extends TypedExp {
    val typ: ExprType = ExprType(this.getClass.getName)
  }

  /** Marker for primitive expression (non query) */
  case class PrimitiveExp(exp: Exp) extends CompilerExp {
    def tresql: String = exp.tresql
  }

  /** Marker for compiler macro, to unwrap compiled result */
  case class PrimitiveDef(exp: Exp, typ: ExprType) extends TypedExp

  //is superclass of sql query and array
  trait RowDefBase extends TypedExp {
    def cols: List[ColDef]

    val typ: ExprType = ExprType(this.getClass.getName)
  }

  //superclass of select and dml statements (insert, update, delete)
  sealed trait SQLDefBase extends RowDefBase {
    def tables: List[TableDef]

    override def tresql = exp match {
      case q: Query =>
        /* FIXME distinct keyword is lost in Cols */
        q.copy(tables = this.tables.map(_.exp),
          cols = Cols(distinct = false, this.cols.map(c => Col(c.exp, c.name)))).tresql
      case x => x.tresql
    }
  }

  //is superclass of insert, update, delete
  trait DMLDefBase extends SQLDefBase {
    def db: Option[String]
  }

  //is superclass of select, union, intersect etc.
  trait SelectDefBase extends SQLDefBase

  case class SelectDef(
                        cols: List[ColDef],
                        tables: List[TableDef],
                        exp: Query
                      ) extends SelectDefBase {
    //check for duplicating tables
    tables.filter { //filter out aliases
      case TableDef(_, Obj(_: TableAlias, _, _, _, _)) => false
      case _ => true
    }.groupBy(_.name).filter(_._2.size > 1) match {
      case d => if (d.nonEmpty) error(
        s"Duplicate table name(s): ${d.keys.mkString(", ")}")
    }
  }

  //union, intersect, except ...
  case class BinSelectDef(
                           leftOperand: SelectDefBase,
                           rightOperand: SelectDefBase,
                           exp: BinOp) extends SelectDefBase {
    private val op =
      if (rightOperand.isInstanceOf[FunSelectDef]) leftOperand
      else if (leftOperand.isInstanceOf[FunSelectDef]) rightOperand
      else leftOperand
    if (!(leftOperand.cols.exists {
      case ColDef(_, All | _: IdentAll, _) => true
      case _ => false
    } || rightOperand.cols.exists {
      case ColDef(_, All | _: IdentAll, _) => true
      case _ => false
    } || leftOperand.isInstanceOf[FunSelectDef]
      || rightOperand.isInstanceOf[FunSelectDef]
      ) && leftOperand.cols.size != rightOperand.cols.size)
      error(
        s"Column count do not match ${leftOperand.cols.size} != ${rightOperand.cols.size}")

    def cols = op.cols

    def tables = op.tables
  }

  //delegates all calls to inner expression
  case class BracesSelectDef(exp: SelectDefBase) extends SelectDefBase {
    override def cols = exp.cols

    override def tables = exp.tables
  }

  // table definition in with [recursive] statement
  case class WithTableDef(
                           cols: List[ColDef],
                           tables: List[TableDef],
                           recursive: Boolean,
                           exp: SQLDefBase
                         ) extends SelectDefBase {
    if (recursive) {
      exp match {
        case _: BinSelectDef =>
        case q => error(s"Recursive table definition must be union, instead found: ${q.tresql}")
      }
    }
  }

  case class ValuesFromSelectDef(exp: SelectDefBase) extends SelectDefBase {
    override def cols = exp.cols

    override def tables = exp.tables
  }

  /** select definition returned from macro or db function, is used in {{{BinSelectDef}}} */
  case class FunSelectDef(
                           cols: List[ColDef],
                           tables: List[TableDef],
                           exp: FunDef
                         ) extends SelectDefBase

  // dml expressions
  case class InsertDef(
                        cols: List[ColDef],
                        tables: List[TableDef],
                        exp: Insert
                      ) extends DMLDefBase {
    override def db: Option[String] = exp.db

    override def tresql = // FIXME alias lost
      exp.copy(table = Ident(List(tables.head.name)),
        cols = this.cols.map(c => Col(c.exp, c.name))).tresql
  }

  case class UpdateDef(
                        cols: List[ColDef],
                        tables: List[TableDef],
                        exp: Update
                      ) extends DMLDefBase {
    override def db: Option[String] = exp.db

    override def tresql = // FIXME alias lost
      exp.copy(table = Ident(List(tables.head.name)),
        cols = this.cols.map(c => Col(c.exp, c.name))).tresql
  }

  case class DeleteDef(
                        tables: List[TableDef],
                        exp: Delete
                      ) extends DMLDefBase {
    def cols = Nil

    override def db: Option[String] = exp.db

    override def tresql = // FIXME alias lost
      exp.copy(table = Ident(List(tables.head.name))).tresql
  }

  case class ReturningDMLDef(
                              cols: List[ColDef],
                              tables: List[TableDef],
                              exp: DMLDefBase
                            ) extends SelectDefBase

  // with [recursive] expressions
  trait WithQuery extends SQLDefBase {
    def exp: SQLDefBase

    def withTables: List[WithTableDef]
  }

  trait WithSelectBase extends SelectDefBase with WithQuery {
    def exp: SelectDefBase
  }

  trait WithDMLQuery extends DMLDefBase with WithQuery {
    def exp: DMLDefBase

    override def db: Option[String] = None
  }

  case class WithSelectDef(
                            exp: SelectDefBase,
                            withTables: List[WithTableDef]
                          ) extends WithSelectBase {
    def cols = exp.cols

    def tables = exp.tables
  }

  case class WithDMLDef(
                         exp: DMLDefBase,
                         withTables: List[WithTableDef]
                       ) extends WithDMLQuery {
    def cols = exp.cols

    def tables = exp.tables
  }

  //array
  case class ArrayDef(cols: List[ColDef]) extends RowDefBase {
    def exp = this

    override def tresql = cols.map(c => QueryParsers.any2tresql(c.col)).mkString("[", ", ", "]")
  }
}
