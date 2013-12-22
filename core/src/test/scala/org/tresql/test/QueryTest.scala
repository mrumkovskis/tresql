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
  class TestFunctions extends Functions {
    def echo(x: String) = x
    def plus(a: java.lang.Long, b: java.lang.Long) = a + b
    def average(a: BigDecimal, b: BigDecimal) = (a + b) / 2
    def dept_desc(d: String, ec: String) = d + " (" + ec + ")"
    def nopars() = "ok"
  }
  Env.functions = new TestFunctions
  Env.cache = new SimpleCache
  Env update ((msg, level) => println (msg))
  Env update ( /*object to table name map*/ Map(
    "emp_dept_view" -> "emp"),
    /*property to column name map*/
    Map("car_usage" -> Map("empname" -> ("empno", "(emp[ename = :empname]{empno})")),
        "car" -> Map("dname" -> ("deptnr", "(case((dept[dname = :dname] {count(deptno)}) = 1, (dept[dname = :dname] {deptno}), -1))"))))
  //create test db script
  new scala.io.BufferedSource(getClass.getResourceAsStream("/db.sql")).mkString.split("//").foreach {
    sql => val st = conn.createStatement; Env.log("Creating database:\n" + sql); st.execute(sql); st.close
  }
  
  def test {
    execStatements
  }
    
  private def execStatements {
    def parsePars(pars: String, sep:String = ";"): Map[String, Any] = {
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
        case A(a, ac) =>
          if (ac.length == 0) List()
          else ac.split(",").map(par).toList
        case x => error("unparseable parameter: " + x)        
      }
      val pl = pars.split(sep).map(par).toList
      if (map) {
        var i = 0
        pl map {
          case (k, v) => (k toString, v)
          case x => i += 1; (i toString, x)
        } toMap
      } else pl.zipWithIndex.map(t => (t._2 + 1).toString -> t._1).toMap
    }
    println("\n------------------------ Test TreSQL statements ----------------------\n")
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
    println("\n---------------- Test API ----------------------\n")
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
    //bind variables absence error message
    assert(intercept[RuntimeException](Query("emp[?]")).getMessage() === "Bind variable with name 1 not found.")
    assert(intercept[RuntimeException](Query("emp[:nr]")).getMessage() === "Bind variable with name nr not found.")
    
    var op = OutPar()
    expectResult(List(10, "x"))(Query("in_out(?, ?, ?)", InOutPar(5), op, "x"))
    expectResult("x")(op.value)
    expectResult(10)(Query.unique[Long]("dept[(deptno = ? | dname ~ ?)]{deptno} @(0 1)", 10, "ACC%"))
    expectResult(None)(Query.headOption[Int]("dept[?]", -1))
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
      "dept[10]{deptno, dname, |emp[deptno = :1(deptno)]{empno, ename, |[empno]work{wdate, hours}#(1,2) work}#(1) emps," +
      " |car[deptnr = :1(deptno)]{name}#(1) cars}"})
    //typed objects tests
    trait Poha
    case class Car(nr: Int, brand: String) extends Poha
    implicit def convertRowLiketoPoha[T <: Poha](r: RowLike, m: Manifest[T]): T = m.toString match {
      case s if s.contains("Car") => Car(r.i.nr, r.s.name).asInstanceOf[T]
      case x => error("Unable to convert to object of type: " + x)
    }
    expectResult(List(Car(1111, "PORCHE"), Car(2222, "BMW"), Car(3333, "MERCEDES"),
        Car(4444, "VOLKSWAGEN")))(Query.list[Car]("car {nr, name} #(1)"))
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
    
    //transformer test
    implicit val transformer: PartialFunction[(String, Any), Any] = {case ("dname", v) => "!" + v}
    expectResult(List(Map("dname" -> "!ACCOUNTING", "emps" -> List(Map("ename" -> "CLARK"),
        Map("ename" -> "KING"), Map("ename" -> "MILLER"))))) {
      Query.toListOfMaps[Map]("dept[10]{dname, |emp{ename}#(1) emps}")
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
        
    println("\n----------- ORT tests ------------\n")
    
    println("\n--- INSERT ---\n")
    
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
    //(work under dept)
    obj = Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "FLORIDA",
        "emp" -> List(Map("empno" -> null, "ename" -> "BROWN", "deptno" -> null),
          Map("empno" -> null, "ename" -> "CHRIS", "deptno" -> null)),
        "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null)))
    expectResult(List(1, List(List(1, 1))))(ORT.insert("dept", obj))

    obj = Map("dname" -> "FOOTBALL", "loc" -> "MIAMI",
        "emp" -> List(Map("ename" -> "BROWN"), Map("ename" -> "CHRIS")))
    expectResult(List(1, List(List(1, 1))))(ORT.insert("dept", obj))
    
    obj = Map("ename" -> "KIKI", "deptno" -> 50, "car"-> List(Map("name"-> "GAZ")))
    expectResult(1)(ORT.insert("emp", obj))
    
    //Ambiguous references to table: emp. Refs: List(Ref(List(empno)), Ref(List(empno_mgr)))
    obj = Map("emp" -> Map("empno" -> null, "ename" -> "BROWN", "deptno" -> null,
            "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null))))
    intercept[Exception](ORT.insert("emp", obj))
    
    //child foreign key is also its primary key
    obj = Map("deptno" -> 60, "dname" -> "POLAR", "loc" -> "ALASKA",
              "dept_addr" -> List(Map("addr" -> "Halibut")))
    expectResult(List(1, List(List(1))))(ORT.insert("dept", obj))
    //child foreign key is also its primary key
    obj = Map("dname" -> "BEACH", "loc" -> "HAWAII",
              "dept_addr" -> List(Map("deptnr" -> 1, "addr" -> "Honolulu", "zip_code" -> "1010")))
    expectResult(List(1, List(List(1))))(ORT.insert("dept", obj))
            
    obj = Map("deptno" -> null, "dname" -> "DRUGS",
              "car" -> List(Map("nr" -> "UUU", "name" -> "BEATLE")))
    expectResult(List(1, List(List(1))))(ORT.insert("dept", obj))
        
    //multiple column primary key
    obj = Map("empno"->7788, "car_nr" -> "1111")
    expectResult(1)(ORT.insert("car_usage", obj))
    //primary key component not specified error must be thrown
    obj = Map("car_nr" -> "1111")
    intercept[SQLException](ORT.insert("car_usage", obj))
    obj = Map("date_from" -> "2013-10-24")
    intercept[SQLException](ORT.insert("car_usage", obj))
    obj = Map("empno" -> 7839)
    intercept[SQLException](ORT.insert("car_usage", obj))
    
    //value clause test
    obj = Map("car_nr" -> 2222, "empname" -> "SCOTT", "date_from" -> "2013-11-06")
    expectResult(1)(ORT.insert("car_usage", obj))
    
    println("\n--- UPDATE ---\n")
    
    obj = Map("dname"->"DEVELOPMENT", "loc"->"DETROIT", "calculated_field"-> 222,
        "emp"->List(
            Map("empno"->null, "ename"->"ANNA", "mgr"->7788, "mgr_name"->null,
              "work:empno"->List(Map("wdate"->"2012-7-9", "hours"->8, "empno_mgr"->7839),
                                 Map("wdate"->"2012-7-10", "hours"->10, "empno_mgr"->7839))), 
            Map("empno"->null, "ename"->"MARY", "mgr"->7566, "mgr_name"->null,
              "work:empno" -> List())),
        "calculated_children"->List(Map("x"->5)), "deptno"->40,
        //this will not be inserted since work has no relation to dept
        "work"->List(Map("wdate"->"2012-7-9", "empno"->7788, "hours"->8, "empno_mgr"->7839),
            Map("wdate"->"2012-7-10", "empno"->7788, "hours"->8, "empno_mgr"->7839)),
            "car" -> List(Map("nr" -> "EEE", "name" -> "BEATLE"), Map("nr" -> "III", "name" -> "FIAT")))
    expectResult(List(1, List(0, List(List(1, List(List(1, 1))),
                                      List(1, List(List()))),
                              0, List(1, 1))))(ORT.update("dept", obj))
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

    //child foreign key is also its primary key
    obj = Map("deptno" -> 60, "dname" -> "POLAR BEAR", "loc" -> "ALASKA",
              "dept_addr" -> List(Map("addr" -> "Halibut", "zip_code" -> "1010")))
    expectResult(List(1, List(1, List(1))))(ORT.update("dept", obj))
    
    //value clause test
    obj = Map("nr" -> 4444, "dname" -> "ACCOUNTING")
    expectResult(1)(ORT.update("car", obj))
    obj = Map("nr" -> 4444, "dname" -> "<NONE>")
    intercept[java.sql.SQLIntegrityConstraintViolationException](ORT.update("car", obj))
    
    println("\n--- DELETE ---\n")
    
    expectResult(1)(ORT.delete("emp", 7934))
    
    println("\n--- SAVE ---\n")
    
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
        
    println("\n-------------- CACHE -----------------\n")
    Env.cache map println
    
  }
    
}
