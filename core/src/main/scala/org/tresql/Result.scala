package org.tresql

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import sys._
import CoreTypes.RowConverter

trait Result extends Iterator[RowLike] with RowLike with TypedResult {

  def columns = (0 to (columnCount - 1)) map column
  def values = (0 to (columnCount - 1)).map(this(_))

  def toListOfMaps: List[Map[String, _]] = this.map(_ => rowToMap).toList
  def toListOfVectors: List[Vector[_]] = this.map(_ => rowToVector).toList

  def rowToMap = (0 to (columnCount - 1)).map(i => column(i).name -> (this(i) match {
    case r: Result => r.toListOfMaps
    case x => x
  })).toMap

  def rowToVector = {
    val b = new scala.collection.mutable.ListBuffer[Any]
    var i = 0
    while (i < columnCount) {
      b += (this(i) match {
        case r: Result => r.toListOfVectors
        case x => x
      })
      i += 1
    }
    Vector(b: _*)
  }

  /**
   * iterates through this result rows as well as these of descendant result
   * ensures execution of dml (update, insert, delete) expressions in colums otherwise
   * has no side effect.
   */
  def execute: Unit = foreach { r =>
    {
      var i = 0
      while (i < columnCount) {
        this(i) match {
          case r: Result => r.execute
          case _ =>
        }
        i += 1
      }
    }
  }

  def close {}

  /** needs to be overriden since super class implementation calls hasNext method */
  override def toString = getClass.toString + ":" + (columns.mkString(","))
}

trait DynamicResult extends Result with DynamicRow with Iterator[DynamicRow]

case class SingleValueResult[T](res: T) extends DynamicResult {
  var n = true
  val cols = List(Column(-1, "value", null))
  def hasNext = if (n) {n = false; true} else false
  def next = this
  def columnCount = 1
  def column(idx: Int) = cols(idx)
  def apply(idx: Int) = res
  def typed[T: Manifest](name: String) = this(name).asInstanceOf[T]
  def apply(name: String) = if (name == "value") res else sys.error("column not found: " + name)
  override def toString = s"SingleValueResult = $res"
}

trait SelectResult extends Result {
  private[tresql] def rs: ResultSet
  private[tresql] def cols: Vector[Column]
  private[tresql] def env: Env
  private[tresql] def sql: String
  private[tresql] def bindVariables: List[Expr]
  private[tresql] def maxSize: Int = 0
  private[tresql] def _columnCount: Int = -1

  private[this] val md = rs.getMetaData
  private[this] val st = rs.getStatement
  private[this] val children = new Array[Any](cols.length)
  private[this] var rsHasNext = true; private[this] var nextCalled = true
  private[this] var closed = false
  private[this] val (colMap, exprCols) = {
    val cwi = cols.zipWithIndex
    (cwi.filter(_._1.name != null).map(t => t._1.name -> t._2).toMap, cwi.filter(_._1.expr != null))
  }
  private[this] var rowCount = 0

  /** calls jdbc result set next method. after jdbc result set next method returns false closes this result */
  def hasNext = {
    if (rsHasNext && nextCalled) {
      rsHasNext = rs.next; nextCalled = false
      if (rsHasNext) {
        var i = 0
        exprCols foreach { c => children(c._2) = c._1.expr() }
      } else close
    }
    rsHasNext
  }
  def next: RowLike = {
    nextCalled = true
    if (maxSize > 0) {
      env.rowCount += 1
      maxSizeControl
    }
    this
  }

  private def maxSizeControl {
    def msg = s"""Result max row count ($maxSize) exceeded. SQL:
      |$sql
      |Bind variables:
      |${bindVariables.mkString(", ")}"""
      .stripMargin
    if (env.rowCount > maxSize) throw new TooManyRowsException(msg)
  }

  def apply(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) asAny(cols(columnIndex).idx)
    else children(columnIndex)
  }
  //fall back to rs.findColumn method in the case hidden column is referenced
  def apply(columnLabel: String) = colMap get columnLabel map (this(_)) getOrElse asAny(rs.findColumn(columnLabel))
  def typed[T:Manifest](columnLabel: String): T = typed[T](colMap(columnLabel))

  def columnCount = if (_columnCount == -1) cols.length else _columnCount
  override def column(idx: Int) = cols(idx)
  def jdbcResult = rs
  override def close {
    if (closed) return
    rs.close
    env.result = null
    if (!env.reusableExpr && st != null) st.close
    closed = true
  }

  //concrete type return methods
  override def int(columnIndex: Int): Int = {
    if (cols(columnIndex).idx != -1) rs.getInt(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[Int]
  }
  override def int(columnLabel: String): Int = int(colMap(columnLabel))
  override def long(columnIndex: Int): Long = {
    if (cols(columnIndex).idx != -1) rs.getLong(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[Long]
  }
  override def long(columnLabel: String): Long = long(colMap(columnLabel))
  override def double(columnIndex: Int): Double = {
    if (cols(columnIndex).idx != -1) rs.getDouble(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[Double]
  }
  override def double(columnLabel: String): Double = double(colMap(columnLabel))
  override def bigdecimal(columnIndex: Int): BigDecimal = {
    if (cols(columnIndex).idx != -1) {
      val bd = rs.getBigDecimal(cols(columnIndex).idx); if (rs.wasNull) null else BigDecimal(bd)
    } else children(columnIndex).asInstanceOf[BigDecimal]
  }
  override def bigdecimal(columnLabel: String): BigDecimal = bigdecimal(colMap(columnLabel))
  override def string(columnIndex: Int): String = {
    if (cols(columnIndex).idx != -1) rs.getString(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[String]
  }
  override def string(columnLabel: String): String = string(colMap(columnLabel))
  override def date(columnIndex: Int): java.sql.Date = {
    if (cols(columnIndex).idx != -1) rs.getDate(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[java.sql.Date]
  }
  override def date(columnLabel: String): java.sql.Date = date(colMap(columnLabel))
  override def timestamp(columnIndex: Int): java.sql.Timestamp = {
    if (cols(columnIndex).idx != -1) rs.getTimestamp(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[java.sql.Timestamp]
  }
  override def timestamp(columnLabel: String): java.sql.Timestamp = timestamp(colMap(columnLabel))
  override def boolean(columnIndex: Int): Boolean = {
    if (cols(columnIndex).idx != -1) rs.getBoolean(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[Boolean]
  }
  override def boolean(columnLabel: String): Boolean = boolean(colMap(columnLabel))
  override def bytes(columnIndex: Int): Array[Byte] = {
    if (cols(columnIndex).idx != -1) rs.getBytes(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[Array[Byte]]
  }
  override def bytes(columnLabel: String) = bytes(colMap(columnLabel))
  override def stream(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) rs.getBinaryStream(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[java.io.InputStream]
  }
  override def stream(columnLabel: String) = stream(colMap(columnLabel))
  override def reader(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) rs.getCharacterStream(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[java.io.Reader]
  }
  override def reader(columnLabel: String) = reader(colMap(columnLabel))
  override def blob(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) rs.getBlob(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[java.sql.Blob]
  }
  override def blob(columnLabel: String) = blob(colMap(columnLabel))
  override def clob(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) rs.getClob(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[java.sql.Clob]
  }
  override def clob(columnLabel: String) = clob(colMap(columnLabel))

  //java type support
  override def jInt(columnIndex: Int): java.lang.Integer = {
    if (cols(columnIndex).idx != -1) {
      val x = rs.getInt(cols(columnIndex).idx)
      if (rs.wasNull()) null else new java.lang.Integer(x)
    } else children(columnIndex).asInstanceOf[java.lang.Integer]
  }
  override def jInt(columnLabel: String): java.lang.Integer = jInt(colMap(columnLabel))
  override def jLong(columnIndex: Int): java.lang.Long = {
    if (cols(columnIndex).idx != -1) {
      val x = rs.getLong(cols(columnIndex).idx)
      if (rs.wasNull()) null else new java.lang.Long(x)
    } else children(columnIndex).asInstanceOf[java.lang.Long]
  }
  override def jLong(columnLabel: String): java.lang.Long = jLong(colMap(columnLabel))
  override def jDouble(columnIndex: Int): java.lang.Double = {
    if (cols(columnIndex).idx != -1) {
      val x = rs.getDouble(cols(columnIndex).idx)
      if (rs.wasNull()) null else new java.lang.Double(x)
    } else children(columnIndex).asInstanceOf[java.lang.Double]
  }
  override def jDouble(columnLabel: String): java.lang.Double = jDouble(colMap(columnLabel))
  override def jBigDecimal(columnIndex: Int): java.math.BigDecimal = {
    if (cols(columnIndex).idx != -1) rs.getBigDecimal(cols(columnIndex).idx)
    else children(columnIndex).asInstanceOf[java.math.BigDecimal]
  }
  override def jBigDecimal(columnLabel: String): java.math.BigDecimal = jBigDecimal(colMap(columnLabel))
  override def jBoolean(columnIndex: Int): java.lang.Boolean = {
    if (cols(columnIndex).idx != -1) {
      val x = rs.getBoolean(cols(columnIndex).idx)
      if (rs.wasNull()) null else new java.lang.Boolean(x)
    } else children(columnIndex).asInstanceOf[java.lang.Boolean]
  }
  override def jBoolean(columnLabel: String): java.lang.Boolean = jBoolean(colMap(columnLabel))

  private def asAny(pos: Int): Any = {
    import java.sql.Types._
    md.getColumnType(pos) match {
      case ARRAY | BINARY | BLOB | DATALINK | DISTINCT | JAVA_OBJECT | LONGVARBINARY | NULL |
        OTHER | REF | STRUCT | VARBINARY => rs.getObject(pos)
      //scala BigDecimal is returned instead of java.math.BigDecimal
      //because it can be easily compared using standart operators (==, <, >, etc)
      case DECIMAL | NUMERIC => {
        val bd = rs.getBigDecimal(pos); if (rs.wasNull) null else BigDecimal(bd)
      }
      case BIGINT | INTEGER | SMALLINT | TINYINT => {
        val v = rs.getLong(pos); if (rs.wasNull) null else v
      }
      case BIT | BOOLEAN =>
        val v = rs.getBoolean(pos); if (rs.wasNull) null else v
      case VARCHAR | CHAR | CLOB | LONGVARCHAR => rs.getString(pos)
      case DATE => rs.getDate(pos)
      case TIME | TIMESTAMP => rs.getTimestamp(pos)
      case DOUBLE | FLOAT | REAL => val v = rs.getDouble(pos); if (rs.wasNull) null else v
    }
  }

}

class DynamicSelectResult private[tresql] (
  private[tresql] val rs: ResultSet,
  private[tresql] val cols: Vector[Column],
  private[tresql] val env: Env,
  private[tresql] val sql: String,
  private[tresql] val bindVariables: List[Expr],
  override private[tresql] val maxSize: Int = 0,
  override private[tresql] val _columnCount: Int = -1
) extends SelectResult with DynamicResult {

  override def next: DynamicRow = super.next.asInstanceOf[DynamicRow]
}

trait ArrayResult extends Result {
  private val cols = (0 until values.size) map { i =>
    Column(i, s"_${i + 1}", null)
  }
  private var _hasNext = true
  def hasNext = if (_hasNext) {
    _hasNext = false
    true
  } else false
  def next: RowLike = this

  def apply(name: String): Any = values(cols.indexWhere(_.name == name))
  def apply(idx: Int): Any = values(idx)
  def column(idx: Int): org.tresql.Column = cols(idx)
  override def columns = cols
  def columnCount: Int = values.size
  def typed[T](name: String)(implicit m: Manifest[T]) = typed[T](name)

  override def hashCode = values.hashCode
  override def equals(obj: Any) = values.equals(obj)

  override def toString = values.mkString("ArrayResult(", ", ", ")")
}

class DynamicArrayResult(override val values: List[Any]) extends ArrayResult with DynamicResult {
  override def next: DynamicRow = super.next.asInstanceOf[DynamicRow]  
}

/**
  {{{CompiledRow}}} is used as superclass for parameter type of {{{CompiledResult[T]}}}
*/
trait CompiledRow extends RowLike with Typed {
  def apply(name: String): Any = ???
  def apply(idx: Int): Any = ???
  def column(idx: Int): org.tresql.Column = ???
  def columnCount: Int = ???
  def columns: Seq[org.tresql.Column] = ???
  def rowToMap: Map[String,Any] = ???
  def values: Seq[Any] = ???
  def typed[T:Manifest](name: String) = ???
}

/**
  {{{CompiledResult}}} is retured from {{{Query.apply[T]}}} method.
  Is used from tresql interpolator macro
*/
trait CompiledResult[T <: RowLike] extends Result with Iterator[T] {

  //better not call super class to list since it creates other subclasses of RowLike
  override def toList: List[T] = foldLeft(List[T]()) {(l, e) => e :: l}.reverse

  def head: T = try hasNext match {
    case true => next
    case false => throw new NoSuchElementException("No rows in result")
  } finally close

  def headOption: Option[T] = try Some(head) catch {
    case e: NoSuchElementException => None
  }

  def unique: T = try hasNext match {
    case true =>
      val v = next
      if (hasNext) error("More than one row for unique result") else v
    case false => error("No rows in result")
  } finally close

  def uniqueOption: Option[T] = try hasNext match {
    case true =>
      val v = next
      if (hasNext) error("More than one row for unique result") else Some(v)
    case false => None
  } finally close
}

case class CompiledSingleValueResult[T <: RowLike](res: T) extends CompiledResult[T] {
  var n = true
  val col = Column(0, "value", null)
  def hasNext = if (n) {n = false; true} else false
  def next = res
  def columnCount = 1
  def column(idx: Int) = col
  def apply(idx: Int) = res(idx)
  def typed[T: Manifest](name: String) = res(name).asInstanceOf[T]
  def apply(name: String) = if (name == "value") res(0) else sys.error("column not found: " + name)
  override def toString = s"SingleValueResult = $res"
}

class CompiledSelectResult[T <: RowLike] private[tresql] (
  private[tresql] val rs: ResultSet,
  private[tresql] val cols: Vector[Column],
  private[tresql] val env: Env,
  private[tresql] val sql: String,
  private[tresql] val bindVariables: List[Expr],
  override private[tresql] val maxSize: Int = 0,
  override private[tresql] val _columnCount: Int = -1,
  private[tresql] val converter: RowConverter[T]
) extends SelectResult with CompiledResult[T] {

  override def next: T = {
    converter(super.next)
  }
}

class CompiledArrayResult[T <: RowLike] private[tresql](
  override val values: List[Any], converter: RowConverter[T])
  extends ArrayResult with CompiledResult[T] {

  override def next: T = {
    converter(super.next)
  }
}

case class Column(idx: Int, name: String, private[tresql] val expr: Expr)

class TooManyRowsException(message: String) extends RuntimeException(message)

trait RowLike extends Typed {
  def apply(idx: Int): Any
  def apply(name: String): Any
  def int(idx: Int) = typed[Int](idx)
  def int(name: String) = typed[Int](name)
  def i(idx: Int) = int(idx)
  def i(name: String) = int(name)
  def long(idx: Int) = typed[Long](idx)
  def long(name: String) = typed[Long](name)
  def l(idx: Int) = long(idx)
  def l(name: String) = long(name)
  def double(idx: Int) = typed[Double](idx)
  def double(name: String) = typed[Double](name)
  def dbl(idx: Int) = double(idx)
  def dbl(name: String) = double(name)
  def bigdecimal(idx: Int) = typed[BigDecimal](idx)
  def bigdecimal(name: String) = typed[BigDecimal](name)
  def bd(idx: Int) = bigdecimal(idx)
  def bd(name: String) = bigdecimal(name)
  def string(idx: Int) = typed[String](idx)
  def string(name: String) = typed[String](name)
  def s(idx: Int) = string(idx)
  def s(name: String) = string(name)
  def date(idx: Int) = typed[java.sql.Date](idx)
  def date(name: String) = typed[java.sql.Date](name)
  def d(idx: Int) = date(idx)
  def d(name: String) = date(name)
  def timestamp(idx: Int) = typed[java.sql.Timestamp](idx)
  def timestamp(name: String) = typed[java.sql.Timestamp](name)
  def t(idx: Int) = timestamp(idx)
  def t(name: String) = timestamp(name)
  def boolean(idx: Int) = typed[Boolean](idx)
  def boolean(name: String) = typed[Boolean](name)
  def bl(idx: Int) = boolean(idx)
  def bl(name: String) = boolean(name)
  def bytes(idx: Int) = typed[Array[Byte]](idx)
  def bytes(name: String) = typed[Array[Byte]](name)
  def b(idx: Int) = bytes(idx)
  def b(name: String) = bytes(name)
  def stream(idx: Int) = typed[java.io.InputStream](idx)
  def stream(name: String) = typed[java.io.InputStream](name)
  def bs(idx: Int) = stream(idx)
  def bs(name: String) = stream(name)
  def blob(idx: Int) = typed[java.sql.Blob](idx)
  def blob(name: String) = typed[java.sql.Blob](name)
  def reader(idx: Int) = typed[java.io.Reader](idx)
  def reader(name: String) = typed[java.io.Reader](name)
  def clob(idx: Int) = typed[java.sql.Clob](idx)
  def clob(name: String) = typed[java.sql.Clob](name)
  def result(idx: Int) = typed[Result](idx)
  def result(name: String) = typed[Result](name)
  def r(idx: Int) = result(idx)
  def r(name: String) = result(name)
  def jInt(idx: Int) = typed[java.lang.Integer](idx)
  def jInt(name: String) = typed[java.lang.Integer](name)
  def ji(idx: Int) = jInt(idx)
  def ji(name: String) = jInt(name)
  def jLong(idx: Int) = typed[java.lang.Long](idx)
  def jLong(name: String) = typed[java.lang.Long](name)
  def jl(idx: Int) = jLong(idx)
  def jl(name: String) = jLong(name)
  def jBoolean(idx: Int) = typed[java.lang.Boolean](idx)
  def jBoolean(name: String) = typed[java.lang.Boolean](name)
  def jbl(idx: Int) = jBoolean(idx)
  def jbl(name: String) = jBoolean(name)
  def jDouble(idx: Int) = typed[java.lang.Double](idx)
  def jDouble(name: String) = typed[java.lang.Double](name)
  def jdbl(idx: Int) = jDouble(idx)
  def jdbl(name: String) = jDouble(name)
  def jBigDecimal(idx: Int) = typed[java.math.BigDecimal](idx)
  def jBigDecimal(name: String) = typed[java.math.BigDecimal](name)
  def jbd(idx: Int) = jBigDecimal(idx)
  def jbd(name: String) = jBigDecimal(name)
  def listOfRows(idx: Int): List[this.type] = this(idx).asInstanceOf[List[this.type]]
  def listOfRows(name: String) = this(name).asInstanceOf[List[this.type]]
  def columnCount: Int
  def rowToMap: Map[String, Any]
  def column(idx: Int): Column
  def columns: Seq[Column]
  def values: Seq[Any]
}

trait DynamicRow extends RowLike with Dynamic {
  /* Dynamic objects */
  /**
   * Wrapper for dynamical result column access as Int
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.i.salary)
   * }}}
   *
   */
  private[tresql] object DynamicInt extends Dynamic {
    def selectDynamic(col: String) = int(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicJInt extends Dynamic {
    def selectDynamic(col: String) = jInt(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as Long
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.l.salary)
   * }}}
   *
   */
  private[tresql] object DynamicLong extends Dynamic {
    def selectDynamic(col: String) = long(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicJLong extends Dynamic {
    def selectDynamic(col: String) = jLong(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as Double
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.dbl.salary)
   * }}}
   *
   */
  private[tresql] object DynamicDouble extends Dynamic {
    def selectDynamic(col: String) = double(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicJDouble extends Dynamic {
    def selectDynamic(col: String) = jDouble(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as BigDecimal
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.bd.salary)
   * }}}
   *
   */
  private[tresql] object DynamicBigDecimal extends Dynamic {
    def selectDynamic(col: String) = bigdecimal(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicJBigDecimal extends Dynamic {
    def selectDynamic(col: String) = jBigDecimal(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as String
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.s.name)
   * }}}
   *
   */
  private[tresql] object DynamicString extends Dynamic {
    def selectDynamic(col: String) = string(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as java.sql.Date
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.d.birthdate)
   * }}}
   *
   */
  private[tresql] object DynamicDate extends Dynamic {
    def selectDynamic(col: String) = date(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as java.sql.Timestamp
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.t.eventTime)
   * }}}
   *
   */
  private[tresql] object DynamicTimestamp extends Dynamic {
    def selectDynamic(col: String) = timestamp(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as Boolean
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.bl.is_active)
   * }}}
   *
   */
  private[tresql] object DynamicBoolean extends Dynamic {
    def selectDynamic(col: String) = boolean(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }

  private[tresql] object DynamicJBoolean extends Dynamic {
    def selectDynamic(col: String) = jBoolean(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  /**
   * Wrapper for dynamical result column access as org.tresql.Result
   * This uses scala.Dynamic feature.
   * Example usages:
   * {{{
   * val result: RowLike = ...
   * println(result.r.childResult)
   * }}}
   *
   */
  private[tresql] object DynamicResult extends Dynamic {
    def selectDynamic(col: String) = result(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  //wrappers for array and stream
  private[tresql] object DynamicByteArray extends Dynamic {
    def selectDynamic(col: String) = bytes(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicStream extends Dynamic {
    def selectDynamic(col: String) = stream(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicBlob extends Dynamic {
    def selectDynamic(col: String) = blob(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicReader extends Dynamic {
    def selectDynamic(col: String) = reader(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  private[tresql] object DynamicClob extends Dynamic {
    def selectDynamic(col: String) = clob(col)
    def applyDynamic(col: String)(args: Any*) = selectDynamic(col)
  }
  def selectDynamic(name: String) = apply(name)
  def applyDynamic(name: String)(args: Any*) = selectDynamic(name)
  def int = DynamicInt
  def i = int
  def long = DynamicLong
  def l = long
  def double = DynamicDouble
  def dbl = double
  def bigdecimal = DynamicBigDecimal
  def bd = bigdecimal
  def string = DynamicString
  def s = string
  def date = DynamicDate
  def d = date
  def timestamp = DynamicTimestamp
  def t = timestamp
  def boolean = DynamicBoolean
  def bl = boolean
  def bytes = DynamicByteArray
  def b = bytes
  def stream = DynamicStream
  def bs = stream
  def blob = DynamicBlob
  def reader = DynamicReader
  def clob = DynamicClob
  def result = DynamicResult
  def r = result
  def jInt = DynamicJInt
  def ji = jInt
  def jLong = DynamicJLong
  def jl = jLong
  def jBoolean = DynamicJBoolean
  def jbl = jBoolean
  def jDouble = DynamicJDouble
  def jdbl = jDouble
  def jBigDecimal = DynamicJBigDecimal
  def jbd = jBigDecimal
}
