package uniso.query.test

import org.scalatest.Suite
import java.sql._
import uniso.query._
import uniso.query.result.Jsonizer._
import scala.util.parsing.json._

class QueryTest extends Suite {
  //initialize environment
  Class.forName("org.hsqldb.jdbc.JDBCDriver")
  val conn = DriverManager.getConnection("jdbc:hsqldb:mem:.")
  Env update conn
  Env update metadata.JDBCMetaData("test", "PUBLIC")
  Env update ((msg, level) => println (msg))
  //create test db script
  new scala.io.BufferedSource(getClass.getResourceAsStream("/db.sql")).mkString.split(";").foreach {
    sql => val st = conn.createStatement; st.execute(sql); st.close
  }
    
  def testStatements {
    def parsePars(pars: String, sep:String = ";"): List[Any] = {
      val DF = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val TF = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val D = """(\d{4}-\d{1,2}-\d{1,2})""".r
      val T = """(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})""".r
      val N = """(-?\d+(\.\d*)?|\d*\.\d+)""".r
      val A = """(\[(.*)\])""".r
      pars.split(sep).map {
        _.trim match {
          case s if (s.startsWith("'")) => s.substring(1, s.length)
          case "null" => null
          case "false" => false
          case "true" => true
          case D(d) => DF.parse(d)
          case T(t) => TF.parse(t)
          case N(n,_) => BigDecimal(n)
          case A(a, ac) => parsePars(ac, ",")
          case x => error("unparseable parameter: " + x)
        }
      } toList
    }

    var nr = 0
    new scala.io.BufferedSource(getClass.getResourceAsStream("/test.txt")).getLines.foreach {
      case l if (l.trim.startsWith("//")) => 
      case l if (l.trim.length > 0) => {
        nr += 1
        val c = l.split("-->")
        val (st, params, patternRes) = (c(0).trim, if (c.length == 2) null else c(1),
             if (c.length == 2) c(1) else c(2))
        println("Executing test #" + nr + ":\n" + st)
        val testRes = if (params == null) Query(st) else Query(st, parsePars(params))
        testRes match {
          case i: Int => println("Result: " + i); assert(i === Integer.parseInt(patternRes.trim))
          case r: Result => {
            val rs = jsonize(testRes, Arrays)
            println("Result: " + rs)
            assert(JSON.parseFull(rs).get === JSON.parseFull(patternRes).get)
          }
          case s: Seq[_] => {
            val rs = jsonize(testRes, Arrays)
            println("Result: " + rs)
            assert(JSON.parseFull(rs).get === JSON.parseFull(patternRes).get)
          }
        }         
      }
      case _ =>
    }
  }
}