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
  Env.dialect = dialects.InsensitiveCmp("ĒŪĪĀŠĢĶĻŽČŅēūīāšģķļžčņ", "EUIASGKLZCNeuiasgklzcn") orElse
    dialects.HSQLDialect
  Env.idExpr = s => "nextval('seq')"
  Env update ((msg, level) => println (msg))
  Env update (/*object to table name map*/Map(
      "emp_dept_view"->"emp"),
      /*property to column name map*/Map(), /*table to name map*/Map(
      "work"->"work[empno]emp{ename || ' (' || wdate || ', ' || hours || ')'}",
      "dept"->"deptno, deptno || ', ' || dname || ' (' || loc || ')'",
      /* NOTE: in emp table name expression in from clause 'dept/emp' emp must come after dept since
        emp name is resolved by adding search by primary key shortcut syntax i.e. 'dept/emp[10]'
        and shortcut primary key search expression refers to the last table in from clause. */
      "emp"->"dept/emp{empno, empno, ename || ' (' || dname || ')'}"),
      /*object property to name map*/Map(
      "emp_dept_view"->Map("deptno"->"null")))
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
        case A(a, ac) => if (ac.length == 0) List() else parsePars(ac, ",")
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
    expectResult(10)(Query.head[Int]("dept{deptno}#(deptno)"))
    expectResult(10)(Query.unique[Int]("dept[10]{deptno}#(deptno)"))
    expectResult(Some(10))(Query.headOption[Int]("dept{deptno}#(deptno)"))
    intercept[Exception](Query.unique[Int]("dept{deptno}#(deptno)"))
    intercept[Exception](Query.unique[Int]("dept[100]{deptno}#(deptno)"))    
    intercept[Exception](Query.head[Int]("dept[100]{deptno}#(deptno)"))    
    expectResult(None)(Query.headOption[Int]("dept[100]{deptno}#(deptno)"))    
    expectResult("ACCOUNTING")(Query.unique[String]("dept[10]{dname}#(deptno)"))
    expectResult("1981-11-17")(Query.unique[java.sql.Date]("emp[sal = 5000]{hiredate}").toString)    
    expectResult(BigDecimal(10))(Query.unique[BigDecimal]("dept[10]{deptno}#(deptno)"))
    expectResult(5)(Query.unique[Int]("inc_val_5(?)", 0))
    expectResult(20)(Query.unique[Int]("inc_val_5(inc_val_5(?))", 10))
    expectResult(15)(Query.unique[Long]("inc_val_5(inc_val_5(?))", 5))
    intercept[Exception](Query.head[Int]("emp[?]{empno}", 'z'))
    //result closing
    intercept[SQLException] {
      val res = Query.select("emp")
      res.toList
      res(0)
    }
    //expression closing
    intercept[SQLException] {
      val ex = Query.build("emp")
      ex.select().toList
      ex.close
      ex.select().toList
    }
    
    var op = OutPar()
    expectResult(List(10, "x"))(Query("in_out(?, ?, ?)", InOutPar(5), op, "x"))
    expectResult("x")(op.value)
    expectResult(10)(Query.unique[Long]("dept[(deptno = ? | dname ~ ?)]{deptno} @(0 1)", 10, "ACC%"))
    expectResult(None)(Query.headOption[Option[_]]("dept[?]", -1))
    //dynamic tests
    expectResult(1900)(Query.select("salgrade[1] {hisal, losal}").foldLeft(0)((x, r) => x + 
        r.i.hisal + r.i.losal))
    expectResult(1900)(Query.select("salgrade[1] {hisal, losal}").foldLeft(0L)((x, r) => x + 
        r.l.hisal + r.l.losal))
    expectResult(1900.00)(Query.select("salgrade[1] {hisal, losal}").foldLeft(0D)((x, r) => x + 
        r.dbl.hisal + r.dbl.losal))
    expectResult(1900)(Query.select("salgrade[1] {hisal, losal}").foldLeft(BigDecimal(0))((x, r) => x + 
        r.bd.hisal + r.bd.losal))
    expectResult("KING PRESIDENT")(Query.select("emp[7839] {ename, job}").foldLeft("")((x, r) => 
        r.s.ename + " " + r.s.job))
    expectResult("1982-12-09")(Query.select("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) => 
        r.d.hiredate.toString))
    expectResult("1982-12-09 00:00:00.0")(Query.select("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) => 
        r.t.hiredate.toString))
    expectResult("KING PRESIDENT")(Query.select("emp[7839] {ename, job}").foldLeft("")((x, r) => 
        r.ename + " " + r.job))
    //typed tests
    expectResult(("MILLER", BigDecimal(2300.35)))(Query.head[(String, BigDecimal)]("emp[hiredate = '1982-01-23']{ename, sal}"))
    expectResult(List(("CLARK", "ACCOUNTING", 2450.00), ("KING", "ACCOUNTING", 5000.00),
      ("MILLER", "ACCOUNTING", 2300.35)))(Query.list[(String, String, Double)]("emp/dept[?]{ename, dname, sal}#(1)", 10))
    expectResult(List(("CLARK", "ACCOUNTING", 2450.00, "NEW YORK"), ("KING", "ACCOUNTING", 5000.00, "NEW YORK"),
      ("MILLER", "ACCOUNTING", 2300.35, "NEW YORK"))) {
      Query.list[String, String, Double, String]("emp/dept[?]{ename, dname, sal, loc}#(1)", 10)
    }
    expectResult(List("ACCOUNTING", "OPERATIONS", "RESEARCH", "SALES"))(Query.list[String]("dept{dname}#(1)"))
    expectResult(List((10,"ACCOUNTING",List((7782,"CLARK",List()), (7839,"KING",List((Date.valueOf("2012-06-06"),3),
        (Date.valueOf("2012-06-07"),4))), (7934, "MILLER", List())),List("PORCHE"))))(
            Query.list[Int, String, List[(Int, String, List[(Date, Int)])], List[String]] {
      "dept[10]{deptno, dname, |emp[deptno = :1(deptno)]{empno, ename, |work[empno = :1(empno)]{wdate, hours}#(1,2) work}#(1) emps," +
      " |car[deptnr = :1(deptno)]{name}#(1) cars}"})
    //column alias test
    expectResult(List(("ACCOUNTING,CLARK", -2450.00), ("ACCOUNTING,KING", -5000.00), ("ACCOUNTING,MILLER", -2300.35))) {
      Query.select("emp/dept[10] {dname || ',' || ename name, -sal salary}#(1)") map (r=> (r.name, r.dbl.salary)) toList
    }
    expectResult(List(0.00, 0.00, 0.00)) {
      Query.select("emp/dept[10] {sal + -sal salary}#(1)") map (_.salary) toList
    }
    expectResult(List(0.00, 0.00, 0.00)) {
      Query.select("emp/dept[10] {(sal + -sal) salary}#(1)") map (_.salary) toList
    }
    
    //bind variables test
    expectResult(List(10, 20, 30, 40)){
      val ex = Query.build("dept[?]{deptno}")
      val res = List(10, 20, 30, 40) flatMap {ex.select(_).map(_.deptno)}
      ex.close
      res
    }
    //arrays, streams test
    expectResult(2)(Query("car_image{carnr, image} + [?, ?], [?, ?]", 1111,
        new java.io.ByteArrayInputStream(scala.Array[Byte](1, 4, 127, -128, 57)), 2222,
        new java.io.ByteArrayInputStream(scala.Array[Byte](0, 32, 100, 99))))
    expectResult(List(1, 4, 127, -128, 57))(
        Query.select("car_image[carnr = ?] {image}", 1111).flatMap(_.b.image).toList)
    expectResult(List[Byte](0, 32, 100, 99)) {
      val res = Query.select("car_image[carnr = ?] {image}", 2222).map(_.bs(0)).toList(0)
      val bytes = new scala.Array[Byte](4)
      res.read(bytes)
      bytes.toList
    }
    expectResult(1)(Query("car_image[carnr = ?]{image} = [?]", 2222,
        new java.io.ByteArrayInputStream(scala.Array[Byte](1, 2, 3, 4, 5, 6, 7))))
    expectResult(List(1, 2, 3, 4, 5, 6, 7))(
        Query.select("car_image[carnr = ?] {image}", 2222).flatMap(_.b("image")).toList)
    //hierarchical inserts, updates test
    expectResult(List(1, List(List(1, 1))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno}[:empno, :ename, :deptno] emps} +
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "DALLAS",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "SMITH", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "LEWIS", "deptno" -> 50)))))
    expectResult(List(1, List(2, List(1, 1))))(Query(
      """dept[:deptno]{deptno, dname, loc,
               -emp[deptno = :deptno],
               +emp {empno, ename, deptno} [:empno, :ename, :deptno] emps} =
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "FLORIDA",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "BROWN", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "CHRIS", "deptno" -> 50)))))
    expectResult(List(2, 1))(Query("emp - [deptno = 50], dept - [50]"))
    expectResult(List(1, List(List(1, 1))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno} [#emp, :ename, :#dept] emps} +
        [#dept, :dname, :loc]""",
      Map("dname" -> "LAW", "loc" -> "DALLAS", "emps" -> scala.Array(
        Map("ename" -> "SMITH"), Map("ename" -> "LEWIS")))))
        
    println("----------- ORT tests ------------")
    println("--- inserts ---")
    var obj:Map[String, Any] = Map("deptno" -> null, "dname" -> "LAW", "loc" -> "DALLAS",
      "calculated_field"->333, "another_calculated_field"->"A",
      "emp" -> scala.Array(Map("empno" -> null, "ename" -> "SMITH", "deptno" -> null,
          "deptno_name" -> List(Map("name" -> "20, RESEARCH (DALLAS)")),
          "work:empno"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null),
              Map("wdate"->"2012-7-10", "empno"->null, "hours"->8, "empno_mgr"->null))),
        Map("empno" -> null, "ename" -> "LEWIS", "deptno" -> null,
            "deptno_name" -> List(Map("name" -> "20, RESEARCH (DALLAS)")),
            "work:empno"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null)))))
    expectResult(List(1, List(List(List(1, List(List(1, 1))), List(1, List(List(1)))))))(ORT.insert("dept", obj))
    intercept[Exception](ORT.insert("no_table", obj))
        
    //insert with set parent id and do not insert existing tables with no link to parent
    //(work under dept) or multiple links to parents (work under emp)
    obj = Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "FLORIDA",
        "emp" -> List(Map("empno" -> null, "ename" -> "BROWN", "deptno" -> null,
            "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null))),
          Map("empno" -> null, "ename" -> "CHRIS", "deptno" -> null,
             "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null)))),
        "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null)))
    expectResult(List(1, List(List(1, 1))))(ORT.insert("dept", obj))
        
    println("--- fill ---")
    //no row found
    obj = Map("empno"-> -1, "ename"->null, "deptno"->null, "deptno_name"->null,
        "mgr"->null, "mgr_name"->null)
    expectResult(None)(ORT.fill("emp", obj, true))
    //no primary key property found
    obj = Map("kuku"-> -1, "ename"->null, "deptno"->null, "deptno_name"->null,
        "mgr"->null, "mgr_name"->null)
    expectResult(None)(ORT.fill("emp", obj, true))
    
    obj = Map("empno"->7788, "ename"->null, "deptno"->null, "deptno_name"->null,
        "mgr"->null, "mgr_name"->null)
    expectResult(Some(Map("ename" -> "SCOTT", "empno" -> 7788, "deptno" -> 20,
        "deptno_name" -> List(Map("name" -> "20, RESEARCH (DALLAS)")), "mgr" -> 7566,
        "mgr_name" -> List(Map("code" -> 7566, "name" -> "JONES (RESEARCH)")))))(ORT.fill("emp", obj, true))
    obj = Map("empno"->7839, "ename"->null, "deptno"->null, "deptno_name"->null,
        "mgr"->null, "mgr_name"->null)
    expectResult(Some(Map("ename" -> "KING", "empno" -> 7839, "deptno" -> 10,
        "deptno_name" -> List(Map("name" -> "10, ACCOUNTING (NEW YORK)")), "mgr" -> null,
        "mgr_name" -> List())))(ORT.fill("emp", obj, true))
        
    obj = Map("deptno"->20, "dname"->null, "loc"->null, "calculated_field"-> 222,
        "emp_dept_view"->List(Map("empno"->7788, "ename"->null, "mgr"->null, "mgr_name"->null,
            "deptno"->null, "deptno_name"->null), 
                  Map("empno"->7566, "ename"->null, "mgr"->null, "mgr_name"->null,
            "deptno"->null, "deptno_name"->null)),
        "calculated_children"->List(Map("x"->5)))
    
    expectResult(Some(Map("deptno" -> 20, "dname" -> "RESEARCH", "loc"->"DALLAS", "calculated_field"-> 222,
        "emp_dept_view" -> List(
            Map("empno" -> 7566, "deptno" -> 20, 
              "mgr_name" -> List(Map("code" -> 7839, "name" -> "KING (ACCOUNTING)")),
              "ename" -> "JONES", "mgr" -> 7839, "deptno_name" -> null), 
            Map("empno" -> 7788, "deptno" -> 20, 
              "mgr_name" -> List(Map("code" -> 7566, "name" -> "JONES (RESEARCH)")),
              "ename" -> "SCOTT", "mgr" -> 7566, "deptno_name" -> null)),
        "calculated_children"->List(Map("x"->5)))))(ORT.fill("dept", obj, true))
        
    obj = Map("deptno"->20, "dname"->null, "loc"->null, "calculated_field"-> 222,
        "emp_dept_view"->List(
            Map("empno"->7788, "ename"->null, "mgr"->null, "mgr_name"->null, "deptno"->20), 
            Map("empno"->7566, "ename"->null, "mgr"->null, "mgr_name"->null, "deptno"->20)),
        "calculated_children"->List(Map("x"->5)))
    
    expectResult(Some(Map("deptno" -> 20, "dname" -> "RESEARCH", "loc"->"DALLAS", "calculated_field"-> 222,
        "emp_dept_view" -> List(
            Map("empno" -> 7566, "mgr_name" -> List(Map("code" -> 7839, "name" -> "KING (ACCOUNTING)")),
              "ename" -> "JONES", "mgr" -> 7839, "deptno"->20), 
            Map("empno" -> 7788, "mgr_name" -> List(Map("code" -> 7566, "name" -> "JONES (RESEARCH)")),
              "ename" -> "SCOTT", "mgr" -> 7566, "deptno"->20)),
        "calculated_children"->List(Map("x"->5)))))(ORT.fill("dept", obj, true))
    
    obj = Map("empno"->7788, "mgr"-> null, "mgr_name"->null, "work:empno"-> Map("wdate"->null,
        "hours"->null, "empno_mgr"->null, "empno_mgr_name"->null))
    expectResult(Some(Map("empno" -> 7788, "mgr" -> 7566, "mgr_name" -> List(Map("code" -> 7566,
      "name" -> "JONES (RESEARCH)")), "work:empno" -> List(Map("wdate" ->
      Date.valueOf("2012-06-06"), "hours" -> 5, "empno_mgr" -> 7566,
      "empno_mgr_name" -> List(Map("code" -> 7566, "name" -> "JONES (RESEARCH)"))),
      Map("wdate" -> Date.valueOf("2012-06-07"), "hours" -> 8, "empno_mgr" -> 7782,
        "empno_mgr_name" -> List(Map("code" -> 7782, "name" -> "CLARK (ACCOUNTING)")))))))(
      ORT.fill("emp", obj, true))

    obj = Map("empno"->7788, "mgr"-> null, "mgr_name"->null, "work"-> Map("wdate"->null,
        "hours"->null, "empno_mgr"->null, "empno_mgr_name"->null))
    //Cannot link child table 'work'. Must be exactly one reference from child to parent table 'emp'.
    //Instead these refs found: List(Ref(List(empno)), Ref(List(empno_mgr)))
    intercept[Exception](ORT.fill("emp", obj, true))

      
    println("--- update ---")
    obj = Map("dname"->"DEVELOPMENT", "loc"->"DETROIT", "calculated_field"-> 222,
        "emp"->List(
            Map("empno"->null, "ename"->"ANNA", "mgr"->7788, "mgr_name"->null, "deptno"->40), 
            Map("empno"->null, "ename"->"MARY", "mgr"->7566, "mgr_name"->null, "deptno"->40)),
        "calculated_children"->List(Map("x"->5)), "deptno"->40,
        "work"->List(Map("wdate"->"2012-7-9", "empno"->7788, "hours"->8, "empno_mgr"->7839),
              Map("wdate"->"2012-7-10", "empno"->7788, "hours"->8, "empno_mgr"->7839)))
    expectResult(List(1, List(0, List(1, 1))))(ORT.update("dept", obj))
    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work:empno"->List(Map("wdate"->"2012-7-9", "empno"->7788, "hours"->8, "empno_mgr"->7839),
              Map("wdate"->"2012-7-10", "empno"->7788, "hours"->8, "empno_mgr"->7839)),
        "calculated_children"->List(Map("x"->5)), "deptno"->40)
    expectResult(List(1, List(2, List(1, 1))))(ORT.update("emp", obj)) 
    //no child record is updated since no relation is found with car and ambiguous relation is
    //found with work
    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work"->List(Map("wdate"->"2012-7-9", "empno"->7788, "hours"->8, "empno_mgr"->7839),
              Map("wdate"->"2012-7-10", "empno"->7788, "hours"->8, "empno_mgr"->7839)),
        "calculated_children"->List(Map("x"->5)), "deptno"->40,
        "car"-> List(Map("nr" -> "AAA", "name"-> "GAZ", "deptno" -> 15)))
    expectResult(1)(ORT.update("emp", obj))    
    
    println("--- delete ---")
    expectResult(1)(ORT.delete("emp", 7934))
    
    println("--- save ---")
    obj = Map("dname" -> "SALES", "loc" -> "WASHINGTON", "calculated_field" -> 222,
      "emp" -> List(
        Map("empno" -> 7499, "ename" -> "ALLEN SMITH", "job" -> "SALESMAN", "mgr" -> 7698,
            "mgr_name" -> null, "deptno" -> 30),
        Map("empno" -> 7654, "ename" -> "MARTIN BLAKE", "job" -> "SALESMAN", "mgr" -> 7698,
            "mgr_name" -> null, "deptno" -> 30),
        Map("empno" -> null, "ename" -> "DEISE ROSE", "job" -> "SALESGIRL", "mgr" -> 7698,
            "mgr_name" -> null, "deptno" -> 30),
        Map("empno" -> 7698, "ename" -> "BLAKE", "job" -> "SALESMAN", "mgr" -> 7839,
            "mgr_name" -> null, "deptno" -> 30)),         
      "calculated_children" -> List(Map("x" -> 5)), "deptno" -> 30)
      expectResult(List(1, List(3, List(1, 1, 1), List(1))))(ORT.save("dept", obj))

    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work:empno"->List(Map("wdate"->"2012-7-12", "empno"->7788, "hours"->10, "empno_mgr"->7839),
              Map("wdate"->"2012-7-13", "empno"->7788, "hours"->3, "empno_mgr"->7839)),
        "calculated_children"->List(Map("x"->5)), "deptno"->20)
    expectResult(List(1, List(2, List(1, 1))))(ORT.save("emp", obj))    
    
    obj = Map("dname"->"DEVELOPMENT", "loc"->"DETROIT", "calculated_field"-> 222,
        "emp"->List(
            Map("empno"->null, "ename"->"AMY", "mgr"->7788, "job"-> "SUPERVIS", "mgr_name"->null, "deptno"->40,
                "work:empno"->List(Map("wdate"->"2012-7-12", "empno"->null, "hours"->5, "empno_mgr"->7839),
              Map("wdate"->"2012-7-13", "empno"->null, "hours"->2, "empno_mgr"->7839))), 
            Map("empno"->null, "ename"->"LENE", "mgr"->7566, "job"-> "SUPERVIS", "mgr_name"->null, "deptno"->40,
                "work:empno"->List(Map("wdate"->"2012-7-12", "empno"->null, "hours"->5, "empno_mgr"->7839),
              Map("wdate"->"2012-7-13", "empno"->null, "hours"->2, "empno_mgr"->7839)))),
        "calculated_children"->List(Map("x"->5)), "deptno"->40)
    expectResult(List(1, List(2, List(List(1, List(0, List(1, 1))), List(1, List(0, List(1, 1)))))))(ORT.save("dept", obj))
    
    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work:empno"->List(), "calculated_children"->List(Map("x"->5)), "deptno"->20)
    expectResult(List(1, List(2)))(ORT.save("emp", obj))        
  }
}
