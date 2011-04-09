package uniso.query

import java.text.SimpleDateFormat

object Functions {
    
    def date(s: String, f: String) = new java.sql.Date(new SimpleDateFormat(f).parse(s).getTime)
    def d(s: String, f: String) = date(s, f)
    def d(s: String) = date(s, "yyyy-MM-dd")
    def dateTime(s: String, f: String) = new java.sql.Timestamp(new SimpleDateFormat(f).parse(s).getTime)
    def dt(s: String, f: String) = dateTime(s, f)
    def dt(s: String) = dateTime(s, "yyyy-MM-dd HH:mm:ss")
    def concat(s: String*) = s mkString
    
    def mkString(res:Result):String = mkString(res, ";")
    def mkString(res:Result, colSep:String):String = mkString(res, colSep, "\n")    
    def mkString(res:Result, colSep:String, rowSep:String) = {
        val sb = new scala.collection.mutable.StringBuilder()
        res foreach {r =>
            var i = 0
            while(i < r.columnCount) {
                sb.append(r(i)).append(colSep)
                i += 1
            }
            sb.delete(sb.length - colSep.length, sb.length).append(rowSep)
        }
        res.close            
        sb.delete(sb.length - rowSep.length, sb.length).toString
    }

    def id(res:Result) = {
        val r = if (res.hasNext) res.next()(0) else null
        res.close
        r
    }
    
}