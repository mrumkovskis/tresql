package uniso.query.result

import java.io.Writer
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.parsing.json.JSONFormat
import uniso.query._

object Jsonizer {
  def jsonize(result: Result, buf: Writer) {
    buf append '['
    var i = 0
    result foreach { r =>
      if (i > 0) buf append ", "
      buf append '{'
      var j = 0
      while (j < r.columnCount) {
        if (j > 0) buf append ", "

        // name
        buf append '"'
        buf append (r.column(j).name match {
          case null => j.toString
          case name => JSONFormat.quoteString(name)
        })
        buf append "\": "

        // value
        r(j) match {
          case r: Result => jsonize(r, buf)
          case n: java.lang.Number => buf append n.toString
          case b: java.lang.Boolean => buf append b.toString
          case d: java.util.Date => buf.append("\"" + toDateString(d) + "\"")
          case null => buf append "null"
          case x => buf.append("\"" + JSONFormat.quoteString(x.toString) + "\"")
        }

        j += 1
      }
      i += 1
      buf append '}'
    }
    buf append ']'
  }

  def toDateString(date: Date) = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)
  }
}
