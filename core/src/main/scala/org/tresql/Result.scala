package org.tresql

import java.sql.ResultSet
import java.sql.ResultSetMetaData

import sys._

class Result private[tresql] (rs: ResultSet, cols: Vector[Column], env: Env)
  extends Iterator[RowLike] with RowLike {
  private[this] val md = rs.getMetaData
  private[this] val colMap = cols.zipWithIndex.filter(_._1.name != null).map(t => t._1.name -> t._2).toMap
  private[this] val row = new Array[Any](cols.length)
  private[this] var rsHasNext = true; private[this] var nextCalled = true
  private[this] var closed = false
  /** calls jdbc result set next method. after jdbc result set next method returns false closes this result */
  def hasNext = {
    if (rsHasNext && nextCalled) {
      rsHasNext = rs.next; nextCalled = false
      if (rsHasNext) {
        var i = 0
        cols foreach { c => if (c.expr != null) row(i) = c.expr(); i += 1 }
      } else {
        close
        closed = true
      }
    }
    rsHasNext
  }
  def next = { nextCalled = true; this }
  def apply(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) asAny(cols(columnIndex).idx)
    else row(columnIndex)
  }
  def typed[T](columnIndex: Int)(implicit m:scala.reflect.Manifest[T]):T = m.toString match {
    case "Int" => int(columnIndex).asInstanceOf[T]
    case "Long" => long(columnIndex).asInstanceOf[T]
    case "Double" => double(columnIndex).asInstanceOf[T]
    case "scala.math.BigDecimal" => bigdecimal(columnIndex).asInstanceOf[T]
    case "java.lang.String" => string(columnIndex).asInstanceOf[T]
    case "java.sql.Date" => date(columnIndex).asInstanceOf[T]
    case "java.sql.Timestamp" => timestamp(columnIndex).asInstanceOf[T]
    case x => apply(columnIndex).asInstanceOf[T]
  }
  def apply(columnLabel: String) = {
    try {
      apply(colMap(columnLabel))
    } catch { case _: NoSuchElementException => asAny(rs.findColumn(columnLabel)) }
  }
  def typed[T](columnLabel: String)(implicit m:scala.reflect.Manifest[T]):T = {
    typed[T](colMap(columnLabel))
  }
  def columnCount = cols.length
  def column(idx: Int) = cols(idx)
  def jdbcResult = rs
  def close {
    if (closed) return
    val st = rs.getStatement
    rs.close
    env.result = null
    if (!env.reusableExpr) {
      st.close
      env.statement = null
    }
  }

  //concrete type return methods
  override def int(columnIndex: Int): Int = {
    if (cols(columnIndex).idx != -1) rs.getInt(cols(columnIndex).idx)
    else row(columnIndex).asInstanceOf[Int]
  }
  override def int(columnLabel: String): Int = {
    try {
      int(colMap(columnLabel))
    } catch { case _: NoSuchElementException => int(rs.findColumn(columnLabel)) }
  }
  override def long(columnIndex: Int): Long = {
    if (cols(columnIndex).idx != -1) rs.getLong(cols(columnIndex).idx)
    else row(columnIndex).asInstanceOf[Long]
  }
  override def long(columnLabel: String): Long = {
    try {
      long(colMap(columnLabel))
    } catch { case _: NoSuchElementException => long(rs.findColumn(columnLabel)) }
  }
  override def double(columnIndex: Int): Double = {
    if (cols(columnIndex).idx != -1) rs.getDouble(cols(columnIndex).idx)
    else row(columnIndex).asInstanceOf[Double]
  }
  override def double(columnLabel: String): Double = {
    try {
      double(colMap(columnLabel))
    } catch { case _: NoSuchElementException => double(rs.findColumn(columnLabel)) }
  }
  override def bigdecimal(columnIndex: Int): BigDecimal = {
    if (cols(columnIndex).idx != -1) {
      val bd = rs.getBigDecimal(cols(columnIndex).idx); if (rs.wasNull) null else BigDecimal(bd)
    } else row(columnIndex).asInstanceOf[BigDecimal]
  }
  override def bigdecimal(columnLabel: String): BigDecimal = {
    try {
      bigdecimal(colMap(columnLabel))
    } catch { case _: NoSuchElementException => bigdecimal(rs.findColumn(columnLabel)) }
  }
  override def string(columnIndex: Int): String = {
    if (cols(columnIndex).idx != -1) rs.getString(cols(columnIndex).idx)
    else row(columnIndex).asInstanceOf[String]
  }
  override def string(columnLabel: String): String = {
    try {
      string(colMap(columnLabel))
    } catch { case _: NoSuchElementException => string(rs.findColumn(columnLabel)) }
  }
  override def date(columnIndex: Int): java.sql.Date = {
    if (cols(columnIndex).idx != -1) rs.getDate(cols(columnIndex).idx)
    else row(columnIndex).asInstanceOf[java.sql.Date]
  }
  override def date(columnLabel: String): java.sql.Date = {
    try {
      date(colMap(columnLabel))
    } catch { case _: NoSuchElementException => date(rs.findColumn(columnLabel)) }
  }
  override def timestamp(columnIndex: Int): java.sql.Timestamp = {
    if (cols(columnIndex).idx != -1) rs.getTimestamp(cols(columnIndex).idx)
    else row(columnIndex).asInstanceOf[java.sql.Timestamp]
  }
  override def timestamp(columnLabel: String): java.sql.Timestamp = {
    try {
      timestamp(colMap(columnLabel))
    } catch { case _: NoSuchElementException => timestamp(rs.findColumn(columnLabel)) }
  }

  override def toList = { val l = (this map (r => Row(this.content))).toList; close; l }

  def content = {
    val b = new scala.collection.mutable.ListBuffer[Any]
    var i = 0
    while (i < columnCount) {
      b += (this(i) match {
        case r: Result => r.toList
        case x => x
      })
      i += 1
    }
    Vector(b: _*)
  }

  /** needs to be overriden since super class implementation calls hasNext method */
  override def toString = getClass.toString + ":" + (cols.mkString(","))

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
      case BIT | BOOLEAN => val v = rs.getBoolean(pos); if (rs.wasNull) null else v
      case VARCHAR | CHAR | CLOB | LONGVARCHAR => rs.getString(pos)
      case DATE => rs.getDate(pos)
      case TIME | TIMESTAMP => rs.getTimestamp(pos)
      case DOUBLE | FLOAT | REAL => val v = rs.getDouble(pos); if (rs.wasNull) null else v
    }
  }

  /*---------------- Single value methods -------------*/
  def head[T](implicit m: scala.reflect.Manifest[T]): T = {
    hasNext match {
      case true => next; val v = typed[T](0); close; v
      case false => throw new NoSuchElementException("No rows in result")
    }
  }
  def headOption[T](implicit m: scala.reflect.Manifest[T]): Option[T] = {
    try {
      Some(head[T])
    } catch {
      case e: NoSuchElementException => None
    }
  }
  def unique[T](implicit m: scala.reflect.Manifest[T]): T = {
    hasNext match {
      case true =>
        next; val v = typed[T](0); if (hasNext) {
          close; error("More than one row for unique result")
        } else v
      case false => error("No rows in result")
    }
  }

  case class Row(row: Seq[Any]) extends RowLike {
    def apply(idx: Int) = row(idx)
    def typed[T](idx: Int)(implicit m:scala.reflect.Manifest[T]) = row(idx).asInstanceOf[T]
    def apply(name: String) = error("unsupported method")
    def typed[T](name: String)(implicit m:scala.reflect.Manifest[T]) = error("unsupported method")
    def content = row
    def columnCount = row.length
    def column(idx: Int) = Result.this.column(idx)
    override def equals(row: Any) = row match {
      case r: RowLike => this.row == r.content
      case r: Seq[_] => this.row == r
      case _ => false
    }
  }
}

trait RowLike extends Dynamic {
  def apply(idx: Int): Any
  def typed[T](idx: Int)(implicit m:scala.reflect.Manifest[T]): T
  def apply(name: String): Any
  def selectDynamic(name:String) = apply(name)
  def applyDynamic(name:String)(args: Any*) = selectDynamic(name) 
  def typed[T](name: String)(implicit m:scala.reflect.Manifest[T]): T
  def int(idx: Int) = typed[Int](idx)
  def int(name: String) = typed[Int](name)
  def int = new DynamicInt(this)
  def i = int
  def long(idx: Int) = typed[Long](idx)
  def long(name: String) = typed[Long](name)
  def long = new DynamicLong(this)
  def l = long
  def double(idx: Int) = typed[Double](idx)
  def double(name: String) = typed[Double](name)
  def double = new DynamicDouble(this)
  def dbl = double
  def bigdecimal(idx: Int) = typed[BigDecimal](idx)
  def bigdecimal(name: String) = typed[BigDecimal](name)
  def bigdecimal = new DynamicBigDecimal(this)
  def bd = bigdecimal
  def string(idx: Int) = typed[String](idx)
  def string(name: String) = typed[String](name)
  def string = new DynamicString(this)
  def s = string
  def date(idx: Int) = typed[java.sql.Date](idx)
  def date(name: String) = typed[java.sql.Date](name)
  def date = new DynamicDate(this)
  def d = date
  def timestamp(idx: Int) = typed[java.sql.Timestamp](idx)
  def timestamp(name: String) = typed[java.sql.Timestamp](name)
  def timestamp = new DynamicTimestamp(this)
  def t = timestamp
  def result(idx: Int) = typed[Result](idx)
  def result(name: String) = typed[Result](name)
  def result = new DynamicResult(this)
  def r = result
  def columnCount: Int
  def content: Seq[Any]
  def column(idx: Int): Column
}

case class Column(val idx: Int, val name: String, private[tresql] val expr: Expr)

/* Dynamic classes */
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
class DynamicInt(row:RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.int(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
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
class DynamicLong(row:RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.long(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
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
class DynamicDouble(row:RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.double(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
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
class DynamicBigDecimal(row: RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.bigdecimal(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
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
class DynamicString(row:RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.string(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
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
class DynamicDate(row:RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.date(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
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
class DynamicTimestamp(row:RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.timestamp(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
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
class DynamicResult(row:RowLike) extends Dynamic {
  def selectDynamic(col:String) = row.result(col)
  def applyDynamic(col:String)(args: Any*) = selectDynamic(col) 
}
