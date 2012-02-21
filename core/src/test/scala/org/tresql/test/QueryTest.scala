package org.tresql.test

import org.scalatest.Suite
import java.sql._
import org.tresql._
import org.tresql.result.Jsonizer._
import scala.util.parsing.json._

import sys._

class QueryTest extends Suite {
  //initialize environment
  Class.forName("org.hsqldb.jdbc.JDBCDriver")
  val conn = DriverManager.getConnection("jdbc:hsqldb:mem:.")
  Env update conn
  Env update ((msg, level) => println (msg))
  //create test db script
  new scala.io.BufferedSource(getClass.getResourceAsStream("/db.sql")).mkString.split(";").foreach {
    sql => val st = conn.createStatement; st.execute(sql); st.close
  }
    
  def testStatements {
    def parsePars(pars: String, sep:String = ";"): Any = {
      val DF = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val TF = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val D = """(\d{4}-\d{1,2}-\d{1,2})""".r
      val T = """(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2})""".r
      val N = """(-?\d+(\.\d*)?|\d*\.\d+)""".r
      val A = """(\[(.*)\])""".r
      val VAR = """(\w+)\s*=\s*(.+)""".r
      var map = false
      def par(p: String): Any = p.trim match {
        case VAR(v, x) => {map = true; (v, par(x))}
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
      val pl = pars.split(sep).map(par).toList
      if (map) {
        var i = 0
        pl map {_ match {
          case t@(k, v) => t
          case x => {i += 1; (i toString, x)}
        }} toMap
      } else pl
    }

    var nr = 0
    new scala.io.BufferedSource(getClass.getResourceAsStream("/test.txt")).getLines.foreach {
      case l if (l.trim.startsWith("//")) => 
      case l if (l.trim.length > 0) => {
        nr += 1
        val (st, params, patternRes) = l.split("-->") match {
          case scala.Array(s, r) => (s, null, r)
          case scala.Array(s, p, r) => (s, p, r)
        }
        println("Executing test #" + nr + ":\n" + st)
        val testRes = if (params == null) Query(st) else Query(st, parsePars(params))
        testRes match {
          case i: Int => println("Result: " + i); assert(i === Integer.parseInt(patternRes.trim))
          case d: BigDecimal => println("Result: " + d); assert(d === BigDecimal(patternRes.trim))
          case s: String => println("Result: " + s); assert(s === patternRes.trim)
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