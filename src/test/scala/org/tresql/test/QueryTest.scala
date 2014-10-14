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
  Env.dialect = dialects.HSQLDialect
  Env.idExpr = s => "nextval('seq')"
  class TestFunctions extends Functions {
    def echo(x: String) = x
    def plus(a: java.lang.Long, b: java.lang.Long) = a + b
    def average(a: BigDecimal, b: BigDecimal) = (a + b) / 2
    def dept_desc(d: String, ec: String) = d + " (" + ec + ")"
    def nopars() = "ok"
  }
  object Macros extends org.tresql.Macros {
    /**
     * Dumb regexp to find bind variables (tresql syntax) in sql string.
     * Expects whitespace, colon, identifier, optional question mark.
     * Whitespace before colon is a workaround to ignore postgresql typecasts.
     */
    private val varRegex = "\\s:[_a-zA-Z]\\w*\\??"r
    override def sql(b: QueryBuilder, const: QueryBuilder#ConstExpr): b.SQLExpr = {
      val value = String.valueOf(const.value)
      val vars = varRegex.findAllIn(value).toList
        .map(_ substring 2)
        .map(v => b.VarExpr(v.replace("?", ""), null, v endsWith "?"))
      val sqlSnippet = varRegex.replaceAllIn(value, " ?")
      if (vars.exists(v => v.opt && !(b.env contains v.name)))
        b.SQLExpr("null", Nil)
      else b.SQLExpr(sqlSnippet, vars)
    }

    def sql_concat(b: QueryBuilder, exprs: Expr*) =
      b.SQLConcatExpr(exprs: _*)

  }  
  Env.functions = new TestFunctions
  Env.macros = Macros
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
    if (util.Properties.versionString contains "2.10") 
      Class.forName("org.tresql.test.TresqlJavaApiTest").newInstance.asInstanceOf[Runnable].run
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
        val testRes = jsonize(if (params == null) Query(st) else Query(st, parsePars(params)), Arrays)
        println("Result: " + testRes)
        assert(JSON.parseFull(testRes).get === JSON.parseFull(patternRes).get)
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
    //option binding
    expectResult("ACCOUNTING")(Query.unique[String]("dept[?]{dname}#(deptno)", Some(10)))
    expectResult("1981-11-17")(Query.unique[java.sql.Date]("emp[sal = 5000]{hiredate}").toString)    
    expectResult(BigDecimal(10))(Query.unique[BigDecimal]("dept[10]{deptno}#(deptno)"))
    expectResult(5)(Query.unique[Int]("inc_val_5(?)", 0))
    expectResult(20)(Query.unique[Int]("inc_val_5(inc_val_5(?))", 10))
    expectResult(15)(Query.unique[Long]("inc_val_5(inc_val_5(?))", 5))
    intercept[Exception](Query.head[Int]("emp[?]{empno}", 'z'))
    //result closing
    intercept[SQLException] {
      val res = Query("emp")
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
    assert(intercept[RuntimeException](Query("emp[?]")).getMessage() === "Missing bind variable: 1")
    assert(intercept[RuntimeException](Query("emp[:nr]")).getMessage() === "Missing bind variable: nr")
    
    var op = OutPar()
    expectResult(List(Vector(List(10, "x"))))(Query("in_out(?, ?, ?)", InOutPar(5), op, "x")
        .toListOfVectors)
    expectResult("x")(op.value)
    expectResult(10)(Query.unique[Long]("dept[(deptno = ? | dname ~ ?)]{deptno} @(0 1)", 10, "ACC%"))
    expectResult(10)(Query.unique[Long]("dept[(deptno = ? | dname ~ ?)]{deptno} @(0 1)",
        Map("1" -> 10, "2" -> "ACC%")))
    expectResult(None)(Query.headOption[Int]("dept[?]", -1))
    //dynamic tests
    expectResult(1900)(Query("salgrade[1] {hisal, losal}").foldLeft(0)((x, r) => x + 
        r.i.hisal + r.i.losal))
    expectResult(1900)(Query("salgrade[1] {hisal, losal}").foldLeft(0L)((x, r) => x + 
        r.l.hisal + r.l.losal))
    expectResult(1900.00)(Query("salgrade[1] {hisal, losal}").foldLeft(0D)((x, r) => x + 
        r.dbl.hisal + r.dbl.losal))
    expectResult(1900)(Query("salgrade[1] {hisal, losal}").foldLeft(BigDecimal(0))((x, r) => x + 
        r.bd.hisal + r.bd.losal))
    expectResult("KING PRESIDENT")(Query("emp[7839] {ename, job}").foldLeft("")((x, r) => 
        r.s.ename + " " + r.s.job))
    expectResult("1982-12-09")(Query("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) => 
        r.d.hiredate.toString))
    expectResult("1982-12-09 00:00:00.0")(Query("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) => 
        r.t.hiredate.toString))
    expectResult("KING PRESIDENT")(Query("emp[7839] {ename, job}").foldLeft("")((x, r) => 
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
    expectResult(List((10, "ACCOUNTING"), (20, "RESEARCH")))(
        Query.list[Int, String]("dept[deptno = ? | deptno = ?]#(1)", 10, 20))
    //typed objects tests
    trait Poha
    case class Car(nr: Int, brand: String) extends Poha
    case class Tyre(carNr: Int, brand: String) extends Poha
    implicit def convertRowLiketoPoha[T <: Poha](r: RowLike, m: Manifest[T]): T = m.toString match {
      case s if s.contains("Car") => Car(r.i.nr, r.s.name).asInstanceOf[T]
      case s if s.contains("Tyre") => Tyre(r.i.nr, r.s.brand).asInstanceOf[T]
      case x => error("Unable to convert to object of type: " + x)
    }
    expectResult(List(Car(1111, "PORCHE"), Car(2222, "BMW"), Car(3333, "MERCEDES"),
        Car(4444, "VOLKSWAGEN")))(Query.list[Car]("car {nr, name} #(1)"))
    expectResult(List(Tyre(3333, "MICHELIN"), Tyre(3333, "NOKIAN")))(
        Query.list[Tyre]("tyres {carnr nr, brand} #(1, 2)"))
    //column alias test
    expectResult(List(("ACCOUNTING,CLARK", -2450.00), ("ACCOUNTING,KING", -5000.00), ("ACCOUNTING,MILLER", -2300.35))) {
      Query("emp/dept[10] {dname || ',' || ename name, -sal salary}#(1)") map (r=> (r.name, r.dbl.salary)) toList
    }
    expectResult(List(0.00, 0.00, 0.00)) {
      Query("emp/dept[10] {sal + -sal salary}#(1)") map (_.salary) toList
    }
    expectResult(List(0.00, 0.00, 0.00)) {
      Query("emp/dept[10] {(sal + -sal) salary}#(1)") map (_.salary) toList
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
    //array, stream, reader, blob, clob test
    expectResult(List(Vector(2)))(Query("car_image{carnr, image} + [?, ?], [?, ?]", 1111,
        new java.io.ByteArrayInputStream(scala.Array[Byte](1, 4, 127, -128, 57)), 2222,
        scala.Array[Byte](0, 32, 100, 99)).toListOfVectors)
    expectResult(List(1, 4, 127, -128, 57))(
        Query("car_image[carnr = ?] {image}", 1111).flatMap(_.b.image).toList)
    expectResult(List[Byte](0, 32, 100, 99)) {
      val res = Query("car_image[carnr = ?] {image}", 2222).map(_.bs(0)).toList(0)
      val bytes = new scala.Array[Byte](4)
      res.read(bytes)
      bytes.toList
    }
    expectResult(List[Byte](0, 32, 100, 99)){
      val b = Query.head[java.sql.Blob]("car_image[carnr = ?] {image}", 2222).getBinaryStream
      Stream.continually(b.read).takeWhile(-1 !=).map(_.toByte).toArray.toList
    }
    expectResult(4){
      Query.head[java.sql.Blob]("car_image[carnr = ?] {image}", 2222).length
    }
    expectResult(List("ACCOUNTING", "OPERATIONS", "RESEARCH", "SALES")) {
      Query.list[java.io.Reader]("dept{dname}#(1)").map(r => 
        new String(Stream.continually(r.read).takeWhile(-1 !=).map(_.toChar).toArray)).toList
    }
    expectResult(List("ACCOUNTING", "OPERATIONS", "RESEARCH", "SALES")) {
      Query.list[java.sql.Clob]("dept{dname}#(1)").map(c => {
        val r = c.getCharacterStream
        new String(Stream.continually(r.read).takeWhile(-1 !=).map(_.toChar).toArray)
      }).toList
    }
    expectResult(List(Vector(1))) {
      Query("+dept_addr", 10, new java.io.StringReader("Strelnieku str."),
          new java.io.StringReader("LV-1010")).toListOfVectors
    }
    expectResult(List(Vector(1)))(Query("car_image[carnr = ?]{image} = [?]", 2222,
        new java.io.ByteArrayInputStream(scala.Array[Byte](1, 2, 3, 4, 5, 6, 7))).toListOfVectors)
    expectResult(List(1, 2, 3, 4, 5, 6, 7))(
        Query("car_image[carnr = ?] {image}", 2222).flatMap(_.b("image")).toList)
    //array binding
    expectResult(List(10, 20, 30))(Query.list[Int]("dept[deptno in ?]{deptno}#(1)", List(30, 20, 10)))
    expectResult(List(10, 20, 30))(Query.list[Int]("dept[deptno in ?]{deptno}#(1)", scala.Array(30, 20, 10)))
    //hierarchical inserts, updates test
    expectResult(List(Vector(List(1, List(List(1, 1))))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno}[:empno, :ename, :deptno] emps} +
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "DALLAS",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "SMITH", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "LEWIS", "deptno" -> 50)))).toListOfVectors)
    expectResult(List(Vector(List(1, List(2, List(1, 1))))))(Query(
      """dept[:deptno]{deptno, dname, loc,
               -emp[deptno = :deptno],
               +emp {empno, ename, deptno} [:empno, :ename, :deptno] emps} =
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "FLORIDA",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "BROWN", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "CHRIS", "deptno" -> 50)))).toListOfVectors)
    expectResult(List(Vector(List(2, 1))))(Query("emp - [deptno = 50], dept - [50]").toListOfVectors)
    expectResult(List(Vector((List(1, List(List((1,10002), (1,10003)))),10001))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno} [#emp, :ename, :#dept] emps} +
        [#dept, :dname, :loc]""",
      Map("dname" -> "LAW", "loc" -> "DALLAS", "emps" -> scala.Array(
        Map("ename" -> "SMITH"), Map("ename" -> "LEWIS")))).toListOfVectors)
      
    //row API
    expectResult(List("CLARK, KING, MILLER"))(Query("dept[10] {dname, |emp {ename}#(1) emps}")
        .toListOfRows.map(r => r.listOfRows("emps").map(_.ename).mkString(", ")))
        
    //tresql string interpolation tests
    var name = "S%"
    expectResult(List(Vector("SCOTT"), Vector("SMITH"), Vector("SMITH"))) {
      tresql"emp [ename ~ $name] {ename}#(1)".toListOfVectors
    }    
    val salary = 1000
    expectResult(List(Vector("SCOTT")))(tresql"emp [ename ~ $name & sal > $salary] {ename}#(1)".toListOfVectors)
    name = null
    expectResult(List(Vector("JAMES"), Vector("SMITH"))) {
      tresql"emp [ename ~ $name? & sal < $salary?] {ename}#(1)".toListOfVectors
    }
    expectResult(List(Vector(0)))(tresql"dummy" toListOfVectors)
     
    
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
    expectResult((List(1, List(List((List(1, List(List(1, 1))),10005), (List(1, List(List(1))),10006)))),10004))(
        ORT.insert("dept", obj))
    intercept[Exception](ORT.insert("no_table", obj))
        
    //insert with set parent id and do not insert existing tables with no link to parent
    //(work under dept)
    obj = Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "FLORIDA",
        "emp" -> List(Map("empno" -> null, "ename" -> "BROWN", "deptno" -> null),
          Map("empno" -> null, "ename" -> "CHRIS", "deptno" -> null)),
        "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null)))
    expectResult((List(1, List(List((1,10007), (1,10008)))),50))(ORT.insert("dept", obj))

    obj = Map("dname" -> "FOOTBALL", "loc" -> "MIAMI",
        "emp" -> List(Map("ename" -> "BROWN"), Map("ename" -> "CHRIS")))
    expectResult((List(1, List(List((1,10010), (1,10011)))),10009))(ORT.insert("dept", obj))
    
    obj = Map("ename" -> "KIKI", "deptno" -> 50, "car"-> List(Map("name"-> "GAZ")))
    expectResult((1,10012))(ORT.insert("emp", obj))
    
    //Ambiguous references to table: emp. Refs: List(Ref(List(empno)), Ref(List(empno_mgr)))
    obj = Map("emp" -> Map("empno" -> null, "ename" -> "BROWN", "deptno" -> null,
            "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null))))
    intercept[Exception](ORT.insert("emp", obj))
    
    //child foreign key is also its primary key
    obj = Map("deptno" -> 60, "dname" -> "POLAR", "loc" -> "ALASKA",
              "dept_addr" -> List(Map("addr" -> "Halibut")))
    expectResult((List(1, List(List(1))),60))(ORT.insert("dept", obj))
    //child foreign key is also its primary key
    obj = Map("dname" -> "BEACH", "loc" -> "HAWAII",
              "dept_addr" -> List(Map("deptnr" -> 1, "addr" -> "Honolulu", "zip_code" -> "1010")))
    expectResult((List(1, List(List(1))),10013))(ORT.insert("dept", obj))
            
    obj = Map("deptno" -> null, "dname" -> "DRUGS",
              "car" -> List(Map("nr" -> "UUU", "name" -> "BEATLE")))
    expectResult((List(1, List(List((1, "UUU")))),10014))(ORT.insert("dept", obj))
        
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
    expectResult(List(1, 
        List(0, 
            List((List(1, List(List(1, 1))),10015), 
                (List(1, List(List())),10016)),
            0, List((1,"EEE"), (1,"III")))))(ORT.update("dept", obj))
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
    
    //update only children (no first level table column updates)
    obj = Map("nr" -> 4444, "tyres" -> List(Map("brand" -> "GOOD YEAR", "season" -> "S"),
        Map("brand" -> "PIRELLI", "season" -> "W")))
    expectResult(List(0, List((1,10017), (1,10018))))(ORT.update("car", obj))
    
    //delete children
    obj = Map("nr" -> 4444, "name" -> "LAMBORGHINI", "tyres" -> List())
    expectResult(List(1, List(2, List())))(ORT.update("car", obj))
    
    //update three level, for the first second level object it's third level is empty
    obj = Map("deptno" -> 10013, "emp" -> List(
        Map("ename" -> "ELKHADY",
            "work:empno" -> List()),
        Map("ename" -> "GUNTER",
            "work:empno" -> List(
            Map("wdate" -> "2014-08-27", "hours" -> 8),
            Map("wdate" -> "2014-08-28", "hours" -> 8)))))
    expectResult(List(0, List((List(1, List(List())), 10019), (List(1, List(List(1, 1))), 10020))))(
      ORT.update("dept", obj))
    //delete third level children
    obj = Map("deptno" -> 10013, "emp" -> List(
        Map("ename" -> "ELKHADY",
            "work:empno" -> List()),
        Map("ename" -> "GUNTER",
            "work:empno" -> List())))
    expectResult(List(2, List((List(1, List(List())), 10021), (List(1, List(List())), 10022))))(
      ORT.update("dept", obj))
      
    //empty column list
    assert(intercept[RuntimeException](
        ORT.update("dept", Map("deptno" -> 10013, "bla" -> 1, "bla1" -> "x")))
        .getMessage.substring(0, 20) === "Column clause empty:")
    
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
      expectResult(List(1, List(3, List(1, 1, 1), List((1,10023)))))(ORT.save("dept", obj))

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
    expectResult(List(1, List(2, List((List(1, List(0, List(1, 1))),10024),
        (List(1, List(0, List(1, 1))),10025)))))(ORT.save("dept", obj))
    
    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work:empno"->List(), "calculated_children"->List(Map("x"->5)), "deptno"->20)
    expectResult(List(1, List(2)))(ORT.save("emp", obj))

    println("\n---- Object INSERT, UPDATE ------\n")
    
    implicit def pohatoMap[T <: Poha](o: T): (String, Map[String, _]) = o match {
      case Car(nr, name) => "car" -> Map("nr" -> nr, "name" -> name)
    }
    expectResult((1,8888))(ORT.insertObj(Car(8888, "OPEL")))
    expectResult(1)(ORT.updateObj(Car(8888, "SAAB")))
    
    println("\n---- TEST tresql methods of QueryParser.Exp ------\n")

    nr = 0
    new scala.io.BufferedSource(getClass.getResourceAsStream("/test.txt"))("UTF-8").getLines.foreach {
      case l if (l.trim.startsWith("//")) =>
      case l if (l.trim.length > 0) =>
        val (st, params, patternRes) = l.split("-->") match {
          case scala.Array(s, r) => (s, null, r)
          case scala.Array(s, p, r) => (s, p, r)
        }
        nr += 1
        println(s"$nr. Testing tresql method of: \n$st")
        QueryParser.parseExp(st) match {
          case e: QueryParser.Exp => assert(e === QueryParser.parseExp(e.tresql))
        }
      case _ =>
    }
    
    println("\n-------------- CACHE -----------------\n")
    Env.cache map println
    
  }
    
}
