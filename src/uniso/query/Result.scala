package uniso.query

import java.sql.ResultSet

class Result private[query] (rs: ResultSet, cols: Vector[Column], reusableStatement: Boolean)
  extends Iterator[RowLike] with RowLike {
  private[this] val colMap = cols.filter(_.name != null).map(c => (c.name, c)).toMap
  private[this] val row = new Array[Any](cols.length)
  private[this] var hn = true; private[this] var flag = true
  def hasNext = {
    if (hn && flag) {
      hn = rs.next; flag = false
      if (hn) {
        var i = 0
        cols foreach { c => if (c.expr != null) row(i) = c.expr(); i += 1 }
      }
    }
    hn
  }
  def next = { flag = true; this }
  def apply(columnIndex: Int) = {
    if (cols(columnIndex).idx != -1) rs.getObject(cols(columnIndex).idx)
    else row(columnIndex)
  }
  def apply(columnLabel: String) = {
    try {
      colMap(columnLabel) match {
        case Column(i, _, null) => rs.getObject(i)
        case Column(i, _, e) => row(i)
      }
    } catch { case _: NoSuchElementException => rs.getObject(columnLabel) }
  }
  def columnCount = cols.length
  def column(idx: Int) = cols(idx)
  def jdbcResult = rs
  def close {
    val st = rs.getStatement
    rs.close
    if (!reusableStatement) st.close
  }

  override def toList = { val l = (this map (r => Row(this.content, this))).toList; close; l }
  
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

}

trait RowLike {
  def apply(idx: Int): Any
  def columnCount: Int
  def content: Seq[Any]
  def column(idx: Int): Column
}

case class Row(row: Seq[Any], result: Result) extends RowLike {
  def apply(idx: Int) = row(idx)
  def content = row
  def columnCount = row.length
  def column(idx: Int) = result.column(idx)
}
case class Column(val idx: Int, val name: String, private[query] val expr: Expr)