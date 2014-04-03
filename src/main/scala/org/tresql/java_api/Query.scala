package org.tresql.java_api

import java.util.{ Map => JMap }
import scala.collection.JavaConversions._
import org.tresql.RowLike

object Query {
  private val q = org.tresql.Query
  def execute(expr: String, params: JMap[String, Object]): Object = {
    q(expr, mapAsScalaMap(params).toMap).asInstanceOf[Object]
  }
  @annotation.varargs
  def execute(expr: String, params: Object*): Object =
    q(expr, params: _*).asInstanceOf[Object]
  def select(expr: String, params: JMap[String, Object]): Result = {
    new ResultWrapper(q.select(expr, mapAsScalaMap(params).toMap))
  }
  @annotation.varargs
  def select(expr: String, params: Object*): Result =
    new ResultWrapper(q.select(expr, params: _*))

  private class RowDelegate(delegate: RowLike) {
    def x = delegate
  }

  private class ResultWrapper(x: org.tresql.Result) extends RowDelegate(x) with Result with RowWrapper {
    override def jdbcResult = x.jdbcResult
    override def close = x.close
    /* TODO
    override def toList = seqAsJavaList(x.toList.map(new RowWrapperImpl(_)))
    override def toListOfMaps
    */
    override def execute = x.execute

    // iterable
    override def iterator = this

    // iterator
    override def hasNext = x.hasNext
    override def next = { x.next; this }
    override def remove { throw new UnsupportedOperationException }
  }

  private trait RowWrapper extends Row { this: RowDelegate =>
    override def get(idx: Int) = x(idx).asInstanceOf[Object] // TODO?
    override def get(name: String) = x(name).asInstanceOf[Object] // TODO?
    override def int_(idx: Int) = x.int(idx)
    override def int_(name: String) = x.int(name)
    override def i(idx: Int) = x.i(idx)
    override def i(name: String) = x.i(name)
    override def long_(idx: Int) = x.long(idx)
    override def long_(name: String) = x.long(name)
    override def l(idx: Int) = x.l(idx)
    override def l(name: String) = x.l(name)
    override def double_(idx: Int) = x.double(idx)
    override def double_(name: String) = x.double(name)
    override def dbl(idx: Int) = x.dbl(idx)
    override def dbl(name: String) = x.dbl(name)
    override def string(idx: Int) = x.string(idx)
    override def string(name: String) = x.string(name)
    override def s(idx: Int) = x.s(idx)
    override def s(name: String) = x.s(name)
    override def date(idx: Int) = x.date(idx)
    override def date(name: String) = x.date(name)
    override def d(idx: Int) = x.d(idx)
    override def d(name: String) = x.d(name)
    override def timestamp(idx: Int) = x.timestamp(idx)
    override def timestamp(name: String) = x.timestamp(name)
    override def t(idx: Int) = x.t(idx)
    override def t(name: String) = x.t(name)
    override def boolean_(idx: Int) = x.boolean(idx)
    override def boolean_(name: String) = x.boolean(name)
    override def bl(idx: Int) = x.bl(idx)
    override def bl(name: String) = x.bl(name)
    override def bytes(idx: Int) = x.bytes(idx)
    override def bytes(name: String) = x.bytes(name)
    override def stream(idx: Int) = x.stream(idx)
    override def stream(name: String) = x.stream(name)
    override def result(idx: Int) = new ResultWrapper(x.result(idx))
    override def result(name: String) = new ResultWrapper(x.result(name))
    override def jInt(idx: Int) = x.jInt(idx)
    override def jInt(name: String) = x.jInt(name)
    override def ji(idx: Int) = x.ji(idx)
    override def ji(name: String) = x.ji(name)
    override def jLong(idx: Int) = x.jLong(idx)
    override def jLong(name: String) = x.jLong(name)
    override def jl(idx: Int) = x.jl(idx)
    override def jl(name: String) = x.jl.name
    override def jBoolean(idx: Int) = x.jBoolean(idx)
    override def jBoolean(name: String) = x.jBoolean(name)
    override def jbl(idx: Int) = x.jbl(idx)
    override def jbl(name: String) = x.jbl(name)
    override def jDouble(idx: Int) = x.jDouble(idx)
    override def jDouble(name: String) = x.jDouble(name)
    override def jdbl(idx: Int) = x.jdbl(idx)
    override def jdbl(name: String) = x.jdbl(name)
    override def jBigDecimal(idx: Int) = x.jBigDecimal(idx)
    override def jBigDecimal(name: String) = x.jBigDecimal(name)
    override def jbd(idx: Int) = x.jbd(idx)
    override def jbd(name: String) = x.jbd(name)
    override def columnCount: Int = x.columnCount
    /* TODO
    override def rowToList: java.util.List[Object]
    */
    override def rowToMap =
      deepMapToJavaMap(x.rowToMap).asInstanceOf[java.util.Map[String, Object]]
    def deepMapToJavaMap(m: Map[String, Any]): java.util.Map[String, Any] =
      mapAsJavaMap(m map {
        case (k, v: Map[String, _]) => (k, deepMapToJavaMap(v))
        case (k, v) => (k, v)
      })
  }
}
