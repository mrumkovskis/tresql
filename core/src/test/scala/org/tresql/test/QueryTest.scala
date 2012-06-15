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
  Env.conn = conn
  Env.dialect = dialects.InsensitiveCmp("ĒŪĪĀŠĢĶĻŽČŅēūīāšģķļžčņ", "EUIASGKLZCNeuiasgklzcn",
      dialects.HSQLDialect)
  Env update ((msg, level) => println (msg))
  //create test db script
  new scala.io.BufferedSource(getClass.getResourceAsStream("/db.sql")).mkString.split("//").foreach {
    sql => val st = conn.createStatement; st.execute(sql); st.close
  }
    
  def test {
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
    println("------------------------ Test TreSQL statements ----------------------")
    var nr = 0
    new scala.io.BufferedSource(getClass.getResourceAsStream("/test.txt"))("UTF-8").getLines.foreach {
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
    println("---------------- Test API ----------------------")
    expect(10)(Query.head[Int]("dept{deptno}#(deptno)"))
    expect(10)(Query.unique[Int]("dept[10]{deptno}#(deptno)"))
    expect(Some(10))(Query.headOption[Int]("dept{deptno}#(deptno)"))
    intercept[Exception](Query.unique[Int]("dept{deptno}#(deptno)"))
    intercept[Exception](Query.unique[Int]("dept[100]{deptno}#(deptno)"))    
    intercept[Exception](Query.head[Int]("dept[100]{deptno}#(deptno)"))    
    expect(None)(Query.headOption[Int]("dept[100]{deptno}#(deptno)"))    
    expect("ACCOUNTING")(Query.unique[String]("dept[10]{dname}#(deptno)"))
    expect("1981-11-17")(Query.unique[java.sql.Date]("emp[sal = 5000]{hiredate}").toString)    
    expect(BigDecimal(10))(Query.unique[BigDecimal]("dept[10]{deptno}#(deptno)"))
    expect(5)(Query.unique[Int]("inc_val_5(?)", 0))
    expect(20)(Query.unique[Int]("inc_val_5(inc_val_5(?))", 10))
    expect(15)(Query.unique[Long]("inc_val_5(inc_val_5(?))", 5))
    var op = OutPar()
    expect(List(10, "x"))(Query("in_out(?, ?, ?)", InOutPar(5), op, "x"))
    expect("x")(op.value)
    expect(10)(Query.unique[Long]("dept[(deptno = ? | dname ~ ?)]{deptno} @(0 1)", 10, "ACC%"))
    expect(None)(Query.first("dept[?]", -1){r => 0})
    //dynamic tests
    expect(1900)(Query.select("salgrade[1] {hisal, losal}").foldLeft(0)((x, r) => x + 
        r.i.hisal + r.i.losal))
    expect(1900)(Query.select("salgrade[1] {hisal, losal}").foldLeft(0L)((x, r) => x + 
        r.l.hisal + r.l.losal))
    expect(1900.00)(Query.select("salgrade[1] {hisal, losal}").foldLeft(0D)((x, r) => x + 
        r.dbl.hisal + r.dbl.losal))
    expect(1900)(Query.select("salgrade[1] {hisal, losal}").foldLeft(BigDecimal(0))((x, r) => x + 
        r.bd.hisal + r.bd.losal))
    expect("KING PRESIDENT")(Query.select("emp[7839] {ename, job}").foldLeft("")((x, r) => 
        r.s.ename + " " + r.s.job))
    expect("1982-12-09")(Query.select("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) => 
        r.d.hiredate.toString))
    expect("1982-12-09 00:00:00.0")(Query.select("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) => 
        r.t.hiredate.toString))
    expect("KING PRESIDENT")(Query.select("emp[7839] {ename, job}").foldLeft("")((x, r) => 
        r.ename + " " + r.job))    
    expect(("MILLER", BigDecimal(2300.35)))(Query.first("emp[hiredate = '1982-01-23']{ename, sal}")
        {r => (r.ename, r.bd.sal)}.get)
    expect("ACCOUNTING")(Query.first("dept[10]")(r => r.dname) orNull)
    expect(("ACCOUNTING", "KING,CLARK,MILLER"))(
      Query.first("dept[10]{deptno, dname, |emp[deptno = :1(1)]{ename} emps}") 
        { r => (r.dname, r.r.emps.map(r => r.ename).mkString(",")) }.get)
    //bind variables test
    val ex = Query.build("dept[?]{deptno}")
    expect(List(10, 20, 30, 40))(List(10, 20, 30, 40) flatMap {ex.select(_).map(_.deptno)})
    ex.close
    //hierarchical inserts, updates test
    expect(List(1, List(List(1, 1))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno}[:empno, :ename, :deptno] emps} +
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "DALLAS",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "SMITH", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "LEWIS", "deptno" -> 50)))))
    expect(List(1, List(2, List(1, 1))))(Query(
      """dept[:deptno]{deptno, dname, loc,
               -emp[deptno = :deptno],
               +emp {empno, ename, deptno} [:empno, :ename, :deptno] emps} =
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "FLORIDA",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "BROWN", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "CHRIS", "deptno" -> 50)))))
    expect(List(2, 1))(Query("emp - [deptno = 50], dept - [50]"))
    expect(List(1, List(List(1, 1))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno} [:empno, :ename, :#seq] emps} +
        [#seq, :dname, :loc]""",
      Map("dname" -> "LAW", "loc" -> "DALLAS", "emps" -> scala.Array(
        Map("empno" -> 1111, "ename" -> "SMITH"), Map("empno" -> 2222, "ename" -> "LEWIS")))))
  }
}
