package uniso.query

import java.text.SimpleDateFormat

object Functions {

  def date(s: String, f: String) = new java.sql.Date(new SimpleDateFormat(f).parse(s).getTime)
  def d(s: String, f: String = "yyyy-MM-dd") = date(s, f)
  def dateTime(s: String, f: String) = new java.sql.Timestamp(new SimpleDateFormat(f).parse(s).getTime)
  def dt(s: String, f: String = "yyyy-MM-dd HH:mm:ss") = dateTime(s, f)
  def concat(s: String*) = s mkString

  def mkString(res: Result): String = mkString(res, ";")
  def mkString(res: Result, colSep: String): String = mkString(res, colSep, "\n")
  def mkString(res: Result, colSep: String, rowSep: String) = {
    val sb = new scala.collection.mutable.StringBuilder()
    res foreach { r => sb.append(r.content.mkString(colSep)).append(rowSep) }
    res.close
    if (sb.length >= rowSep.length) sb.delete(sb.length - rowSep.length, sb.length).toString
    else sb.toString
  }

  def id(res: Result) = {
    val r = if (res.hasNext) res.next()(0) else null
    res.close
    r
  }

}