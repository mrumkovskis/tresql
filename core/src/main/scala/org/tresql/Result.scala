package org.tresql

import java.sql.ResultSet
import java.sql.ResultSetMetaData

import sys._

class Result private[tresql] (rs: ResultSet, cols: Vector[Column], env: Env)
  extends Iterator[RowLike] with RowLike {
  private[this] val md = rs.getMetaData
  private[this] val colMap = cols.zipWithIndex.filter(_._1.name != null).map(t => t._1.name -> t._2).toMap
  private[this] val row = new Array[Any](cols.length)
  private[this] var hn = true; private[this] var flag = true
  private[this] var closed = false
  /** calls jdbc result set next method. after jdbc result set next method returns false closes this result */
  def hasNext = {
    if (hn && flag) {
      hn = rs.next; flag = false
      if (hn) {
        var i = 0
        cols foreach { c => if (c.expr != null) row(i) = c.expr(); i += 1 }
      } else {
        close
        closed = true
      }
    }
    hn
  }
  def next = { flag = true; this }
  def apply(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) asAny(cols(columnIndex).idx)
    else row(columnIndex)
  }
  def apply(columnLabel: String) = {
    try {
      apply(colMap(columnLabel))
    } catch { case _: NoSuchElementException => asAny(rs.findColumn(columnLabel)) }
  }
  def columnCount = cols.length
  def column(idx: Int) = cols(idx)
  def jdbcResult = rs
  def close {
    if (closed) return
    val st = rs.getStatement
    rs.close
    env update (null: Result)
    if (!env.reusableExpr) {
      st.close
      env update (null: java.sql.PreparedStatement)
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
      int(colMap(columnLabel))
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

  case class Row(row: Seq[Any]) extends RowLike {
    def apply(idx: Int) = row(idx)
    def apply(name: String) = error("unsupported method")
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

trait RowLike {
  def apply(idx: Int): Any
  //this can be renamed to apply, when scala compiler provides overloaded methods with default parameters
  def typed[T](idx: Int, conv: (Any) => T = (v: Any) => v.asInstanceOf[T]): T = conv(apply(idx))
  def apply(name: String): Any
  def apply[T](name: String, conv: (Any) => T = (v: Any) => v.asInstanceOf[T]): T = apply(name).asInstanceOf[T]
  def int(idx: Int) = apply(idx).asInstanceOf[Int]
  def int(name: String) = apply(name).asInstanceOf[Int]
  def long(idx: Int) = apply(idx).asInstanceOf[Long]
  def long(name: String) = apply(name).asInstanceOf[Long]
  def double(idx: Int) = apply(idx).asInstanceOf[Double]
  def double(name: String) = apply(name).asInstanceOf[Double]
  def bigdecimal(idx: Int) = apply(idx).asInstanceOf[BigDecimal]
  def bigdecimal(name: String) = apply(name).asInstanceOf[BigDecimal]
  def string(idx: Int) = apply(idx).asInstanceOf[String]
  def string(name: String) = apply(name).asInstanceOf[String]
  def date(idx: Int) = apply(idx).asInstanceOf[java.sql.Date]
  def date(name: String) = apply(name).asInstanceOf[java.sql.Date]
  def timestamp(idx: Int) = apply(idx).asInstanceOf[java.sql.Timestamp]
  def timestamp(name: String) = apply(name).asInstanceOf[java.sql.Timestamp]
  def columnCount: Int
  def content: Seq[Any]
  def column(idx: Int): Column
}

case class Column(val idx: Int, val name: String, private[tresql] val expr: Expr)
