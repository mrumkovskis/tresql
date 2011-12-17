package org.tresql.result

import java.io.Writer
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.parsing.json.JSONFormat
import org.tresql._

object Jsonizer {

  sealed class ResultType
  case object Objects extends ResultType
  case object Arrays extends ResultType
  case object Object extends ResultType

  class ResultTypeException(message: String)
    extends RuntimeException(message: String)

  def jsonize(result: Any, buf: Writer, rType: ResultType = Objects) {
    result match {
      case r: Result =>
        var done = false;
        try {
          jsonizeResult(r, buf, rType)
          done = true
        } finally {
          try {
            r.close
          } catch {
            case e: Exception => if (done) {
              throw e
            } else {
              // exception while closing result on top of another exception
              // must not lose the first exception - can not throw
              // TODO log low priority?
            }
          }
        }
      case a: Seq[_] => jsonizeArray(a, buf, rType)
      case a: Array[_] => jsonizeArray(a, buf, rType)
      case b: Boolean => buf append b.toString
      case n: Byte => buf append n.toString
      case n: Short => buf append n.toString
      case n: Int => buf append n.toString
      case n: Long => buf append n.toString
      case n: Float => buf append n.toString
      case n: Double => buf append n.toString
      case n: scala.math.BigInt => buf append n.toString
      case n: scala.math.BigDecimal => buf append n.toString
      case n: java.lang.Number => buf append n.toString
      case b: java.lang.Boolean => buf append b.toString
      case t: java.sql.Timestamp => //
        buf.append("\"" + t.toString.substring(0, 19) + "\"")
      case d: java.sql.Date => buf.append("\"" + d.toString + "\"")
      case d: java.util.Date => buf.append("\"" + format(d) + "\"")
      case null => buf append "null"
      case x => buf.append("\"" + JSONFormat.quoteString(x.toString) + "\"")
    }
  }
  
  def jsonize(result: Any, rType: ResultType): String = {
    val w = new java.io.StringWriter
    jsonize(result, w, rType)
    w.toString
  }

  def jsonizeResult(result: Result, buf: Writer, rType: ResultType = Objects) {
    if (rType != Object) buf append '['
    var i = 0
    result foreach { r =>
      if (i > 0 && rType == Object)
        throw new ResultTypeException("Failed to jsonize result: " +
          "single-object result requested, but result has more than one")
      if (i > 0) buf append ", "
      buf append (if (rType == Arrays) '[' else '{')
      var j = 0
      while (j < r.columnCount) {
        if (j > 0) buf append ", "

        // name
        if (rType != Arrays) {
          buf append '"'
          buf append (r.column(j).name match {
            case null => j.toString
            case name => JSONFormat.quoteString(name)
          })
          buf append "\": "
        }

        // value
        jsonize(r(j), buf, if (rType == Object) Arrays else rType)

        j += 1
      }
      i += 1
      buf append (if (rType == Arrays) ']' else '}')
    }
    if (rType != Object) buf append ']'
  }

  def jsonizeArray(result: Iterable[_], buf: Writer, rType: ResultType = Objects) {
    buf append '['
    var i = 0
    result foreach { r =>
      if (i > 0) buf append ", "
      // name
      if (rType != Arrays) {
        buf append '"'
        buf append i.toString
        buf append "\": "          
      }
      // value
      jsonize(r, buf, if (rType == Object) Arrays else rType)
      i += 1
    }
    buf append ']'
  }

  def format(date: Date) = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
  }
}
