package uniso.query

import java.sql.ResultSet

//private[query] modifier results in runtime error :(
class Result(private val rs: ResultSet, private val cols: Vector[Column]) extends Iterator[Result] {
    private[this] val colMap = cols.filter(_.name != null).map(c => (c.name, c)).toMap
    private[this] val row = new Array[Any](cols.length)
    private[this] var hn = true; private[this] var flag = true
    def hasNext = {
        if (hn && flag) {
            hn = rs.next; flag = false
            if (hn) {
                var i = 0
                cols foreach {c => if (c.expr != null) row(i) = c.expr(); i += 1}
            }
        }
        hn
    }
    def next = {flag = true; this}
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
        } catch {case _:NoSuchElementException => rs.getObject(columnLabel)}
    }
    def columnCount = cols.length
    def column(idx:Int) = cols(idx)
    def jdbcResult = rs
    def close {
        val st = rs.getStatement
        rs.close
        st.close
    }
    /** needs to be overriden since super class implementation calls hasNext method */
    override def toString = getClass.toString + ":" + (cols.mkString(","))

}

case class Column(val idx: Int, val name: String, val expr: Expr)