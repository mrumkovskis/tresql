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
        .map(v => b.VarExpr(v.replace("?", ""), Nil, null, v endsWith "?"))
      val sqlSnippet = varRegex.replaceAllIn(value, " ?")
      if (vars.exists(v => v.opt && !(b.env contains v.name)))
        b.SQLExpr("null", Nil)
      else b.SQLExpr(sqlSnippet, vars)
    }
    def null_macros(b: QueryBuilder) = null
    def sql_concat(b: QueryBuilder, exprs: Expr*) =
      b.SQLConcatExpr(exprs: _*)
    def if_defined(b: QueryBuilder, v: QueryBuilder#VarExpr, e: Expr) =
      if (b.env contains v.name) e else null
    def if_missing(b: QueryBuilder, v: QueryBuilder#VarExpr, e: Expr) =
      if (b.env contains v.name) null else e

  }
  Env.functions = new TestFunctions
  Env.macros = Macros
  Env.cache = new SimpleCache(-1)
  Env.logger = ((msg, level) => println (msg))
  Env updateValueExprs (
    /*value expr*/
    Map(("car_usage" -> "empno") -> "(emp[empno = :empno]{empno})",
        ("car" -> "deptnr") -> "(case((dept[deptno = :deptnr] {count(deptno)}) = 1, (dept[deptno = :deptnr] {deptno}), -1))",
        ("tyres_usage" -> "carnr") -> ":#car"))
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
        println(s"Executing test #$nr:")
        val testRes = jsonize(if (params == null) Query(st) else Query(st, parsePars(params)), Arrays)
        println("Result: " + testRes)
        assert(JSON.parseFull(testRes).get === JSON.parseFull(patternRes).get)
      }
      case _ =>
    }
    println("\n---------------- Test API ----------------------\n")
    assertResult(10)(Query.head[Int]("dept{deptno}#(deptno)"))
    assertResult(10)(Query.unique[Int]("dept[10]{deptno}#(deptno)"))
    assertResult(Some(10))(Query.headOption[Int]("dept{deptno}#(deptno)"))
    intercept[Exception](Query.unique[Int]("dept{deptno}#(deptno)"))
    intercept[Exception](Query.unique[Int]("dept[100]{deptno}#(deptno)"))
    intercept[Exception](Query.head[Int]("dept[100]{deptno}#(deptno)"))
    assertResult(None)(Query.headOption[Int]("dept[100]{deptno}#(deptno)"))
    assertResult("ACCOUNTING")(Query.unique[String]("dept[10]{dname}#(deptno)"))
    //option binding
    assertResult("ACCOUNTING")(Query.unique[String]("dept[?]{dname}#(deptno)", Some(10)))
    assertResult("1981-11-17")(Query.unique[java.sql.Date]("emp[sal = 5000]{hiredate}").toString)
    assertResult(BigDecimal(10))(Query.unique[BigDecimal]("dept[10]{deptno}#(deptno)"))
    assertResult(5)(Query.unique[Int]("inc_val_5(?)", 0))
    assertResult(20)(Query.unique[Int]("inc_val_5(inc_val_5(?))", 10))
    assertResult(15)(Query.unique[Long]("inc_val_5(inc_val_5(?))", 5))
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
      ex().asInstanceOf[Result].toList
      ex.close
      ex().asInstanceOf[Result].toList
    }
    //bind variables absence error message
    assert(intercept[MissingBindVariableException](Query("emp[?]")).name === "1")
    assert(intercept[MissingBindVariableException](Query("emp[:nr]")).name === "nr")

    var op = OutPar()
    assertResult(List(Vector(List(10, "x"))))(Query("in_out(?, ?, ?)", InOutPar(5), op, "x")
        .toListOfVectors)
    assertResult("x")(op.value)
    assertResult(10)(Query.unique[Long]("dept[(deptno = ? | dname ~ ?)]{deptno} @(0 1)", 10, "ACC%"))
    assertResult(10)(Query.unique[Long]("dept[(deptno = ? | dname ~ ?)]{deptno} @(0 1)",
        Map("1" -> 10, "2" -> "ACC%")))
    assertResult(None)(Query.headOption[Int]("dept[?]", -1))
    //dynamic tests
    assertResult(1900)(Query("salgrade[1] {hisal, losal}").foldLeft(0)((x, r) => x +
        r.i.hisal + r.i.losal))
    assertResult(1900)(Query("salgrade[1] {hisal, losal}").foldLeft(0L)((x, r) => x +
        r.l.hisal + r.l.losal))
    assertResult(1900.00)(Query("salgrade[1] {hisal, losal}").foldLeft(0D)((x, r) => x +
        r.dbl.hisal + r.dbl.losal))
    assertResult(1900)(Query("salgrade[1] {hisal, losal}").foldLeft(BigDecimal(0))((x, r) => x +
        r.bd.hisal + r.bd.losal))
    assertResult("KING PRESIDENT")(Query("emp[7839] {ename, job}").foldLeft("")((x, r) =>
        r.s.ename + " " + r.s.job))
    assertResult("1982-12-09")(Query("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) =>
        r.d.hiredate.toString))
    assertResult("1982-12-09 00:00:00.0")(Query("emp[ename ~~ 'scott'] {hiredate}").foldLeft("")((x, r) =>
        r.t.hiredate.toString))
    assertResult("KING PRESIDENT")(Query("emp[7839] {ename, job}").foldLeft("")((x, r) =>
        r.ename + " " + r.job))
    //typed tests
    assertResult(("MILLER", BigDecimal(2300.35)))(Query.head[(String, BigDecimal)]("emp[hiredate = '1982-01-23']{ename, sal}"))
    assertResult(List(("CLARK", "ACCOUNTING", 2450.00), ("KING", "ACCOUNTING", 5000.00),
      ("MILLER", "ACCOUNTING", 2300.35)))(Query.list[(String, String, Double)]("emp/dept[?]{ename, dname, sal}#(1)", 10))
    assertResult(List(("CLARK", "ACCOUNTING", 2450.00, "NEW YORK"), ("KING", "ACCOUNTING", 5000.00, "NEW YORK"),
      ("MILLER", "ACCOUNTING", 2300.35, "NEW YORK"))) {
      Query.list[String, String, Double, String]("emp/dept[?]{ename, dname, sal, loc}#(1)", 10)
    }
    assertResult(List("ACCOUNTING", "OPERATIONS", "RESEARCH", "SALES"))(Query.list[String]("dept{dname}#(1)"))
    assertResult(List((10,"ACCOUNTING",List((7782,"CLARK",List()), (7839,"KING",List((Date.valueOf("2012-06-06"),3),
        (Date.valueOf("2012-06-07"),4))), (7934, "MILLER", List())),List("PORCHE"))))(
            Query.list[Int, String, List[(Int, String, List[(Date, Int)])], List[String]] {
      "dept[10]{deptno, dname, |emp[deptno = :1(deptno)]{empno, ename, |[empno]work{wdate, hours}#(1,2) work}#(1) emps," +
      " |car[deptnr = :1(deptno)]{name}#(1) cars}"})
    assertResult(List((10, "ACCOUNTING"), (20, "RESEARCH")))(
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
    assertResult(List(Car(1111, "PORCHE"), Car(2222, "BMW"), Car(3333, "MERCEDES"),
        Car(4444, "VOLKSWAGEN")))(Query.list[Car]("car {nr, name} #(1)"))
    assertResult(List(Tyre(3333, "MICHELIN"), Tyre(3333, "NOKIAN")))(
        Query.list[Tyre]("tyres {carnr nr, brand} #(1, 2)"))
    //column alias test
    assertResult(List(("ACCOUNTING,CLARK", -2450.00), ("ACCOUNTING,KING", -5000.00), ("ACCOUNTING,MILLER", -2300.35))) {
      Query("emp/dept[10] {dname || ',' || ename name, -sal salary}#(1)") map (r=> (r.name, r.dbl.salary)) toList
    }
    assertResult(List(0.00, 0.00, 0.00)) {
      Query("emp/dept[10] {sal + -sal salary}#(1)") map (_.salary) toList
    }
    assertResult(List(0.00, 0.00, 0.00)) {
      Query("emp/dept[10] {(sal + -sal) salary}#(1)") map (_.salary) toList
    }

    //transformer test
    implicit val transformer: PartialFunction[(String, Any), Any] = {case ("dname", v) => "!" + v}
    assertResult(List(Map("dname" -> "!ACCOUNTING", "emps" -> List(Map("ename" -> "CLARK"),
        Map("ename" -> "KING"), Map("ename" -> "MILLER"))))) {
      Query.toListOfMaps[Map]("dept[10]{dname, |emp{ename}#(1) emps}")
    }

    //bind variables test
    assertResult(List(10, 20, 30, 40)){
      val ex = Query.build("dept[?]{deptno}")
      val res = List(10, 20, 30, 40) flatMap {p=> ex(List(p)).asInstanceOf[Result].map(_.deptno)}
      ex.close
      res
    }
    //array, stream, reader, blob, clob test
    assertResult(List(Vector(2)))(Query("car_image{carnr, image} + [?, ?], [?, ?]", 1111,
        new java.io.ByteArrayInputStream(scala.Array[Byte](1, 4, 127, -128, 57)), 2222,
        scala.Array[Byte](0, 32, 100, 99)).toListOfVectors)
    assertResult(List(1, 4, 127, -128, 57))(
        Query("car_image[carnr = ?] {image}", 1111).flatMap(_.b.image).toList)
    assertResult(List[Byte](0, 32, 100, 99)) {
      val res = Query("car_image[carnr = ?] {image}", 2222).map(_.bs(0)).toList(0)
      val bytes = new scala.Array[Byte](4)
      res.read(bytes)
      bytes.toList
    }
    assertResult(List[Byte](0, 32, 100, 99)){
      val b = Query.head[java.sql.Blob]("car_image[carnr = ?] {image}", 2222).getBinaryStream
      Stream.continually(b.read).takeWhile(-1 !=).map(_.toByte).toArray.toList
    }
    assertResult(4){
      Query.head[java.sql.Blob]("car_image[carnr = ?] {image}", 2222).length
    }
    assertResult(List("ACCOUNTING", "OPERATIONS", "RESEARCH", "SALES")) {
      Query.list[java.io.Reader]("dept{dname}#(1)").map(r =>
        new String(Stream.continually(r.read).takeWhile(-1 !=).map(_.toChar).toArray)).toList
    }
    assertResult(List("ACCOUNTING", "OPERATIONS", "RESEARCH", "SALES")) {
      Query.list[java.sql.Clob]("dept{dname}#(1)").map(c => {
        val r = c.getCharacterStream
        new String(Stream.continually(r.read).takeWhile(-1 !=).map(_.toChar).toArray)
      }).toList
    }
    assertResult(List(Vector(1))) {
      Query("+dept_addr", 10, new java.io.StringReader("Strelnieku str."),
          new java.io.StringReader("LV-1010"), null).toListOfVectors
    }
    assertResult(List(Vector(1)))(Query("car_image[carnr = ?]{image} = [?]", 2222,
        new java.io.ByteArrayInputStream(scala.Array[Byte](1, 2, 3, 4, 5, 6, 7))).toListOfVectors)
    assertResult(List(1, 2, 3, 4, 5, 6, 7))(
        Query("car_image[carnr = ?] {image}", 2222).flatMap(_.b("image")).toList)
    //array binding
    assertResult(List(10, 20, 30))(Query.list[Int]("dept[deptno in ?]{deptno}#(1)", List(30, 20, 10)))
    assertResult(List(10, 20, 30))(Query.list[Int]("dept[deptno in ?]{deptno}#(1)", scala.Array(30, 20, 10)))
    //hierarchical inserts, updates test
    assertResult(List(Vector(List(1, List(List(1, 1))))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno}[:empno, :ename, :deptno] emps} +
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "DALLAS",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "SMITH", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "LEWIS", "deptno" -> 50)))).toListOfVectors)
    assertResult(List(Vector(List(1, List(2, List(1, 1))))))(Query(
      """dept[:deptno]{deptno, dname, loc,
               -emp[deptno = :deptno],
               +emp {empno, ename, deptno} [:empno, :ename, :deptno] emps} =
        [:deptno, :dname, :loc]""",
      Map("deptno" -> 50, "dname" -> "LAW", "loc" -> "FLORIDA",
        "emps" -> List(Map("empno" -> 1111, "ename" -> "BROWN", "deptno" -> 50),
          Map("empno" -> 2222, "ename" -> "CHRIS", "deptno" -> 50)))).toListOfVectors)
    assertResult(List(Vector(List(2, 1))))(Query("emp - [deptno = 50], dept - [50]").toListOfVectors)
    assertResult(List(Vector((List(1, List(List((1,10002), (1,10003)))),10001))))(Query(
      """dept{deptno, dname, loc, +emp {empno, ename, deptno} [#emp, :ename, :#dept] emps} +
        [#dept, :dname, :loc]""",
      Map("dname" -> "LAW", "loc" -> "DALLAS", "emps" -> scala.Array(
        Map("ename" -> "SMITH"), Map("ename" -> "LEWIS")))).toListOfVectors)

    //row API
    assertResult(List("CLARK, KING, MILLER"))(Query("dept[10] {dname, |emp {ename}#(1) emps}")
        .toListOfRows.map(r => r.listOfRows("emps").map(_.ename).mkString(", ")))

    //tresql string interpolation tests
    var name = "S%"
    assertResult(List(Vector("SCOTT"), Vector("SMITH"), Vector("SMITH"))) {
      tresql"emp [ename ~ $name] {ename}#(1)".toListOfVectors
    }
    val salary = 1000
    assertResult(List(Vector("SCOTT")))(tresql"emp [ename ~ $name & sal > $salary] {ename}#(1)".toListOfVectors)
    name = null
    assertResult(List(Vector("JAMES"), Vector("SMITH"))) {
      tresql"emp [ename ~ $name? & sal < $salary?] {ename}#(1)".toListOfVectors
    }
    assertResult(List(Vector(0)))(tresql"dummy" toListOfVectors)


    println("\n----------- ORT tests ------------\n")

    println("\n--- INSERT ---\n")

    var obj:Map[String, Any] = Map("deptno" -> null, "dname" -> "LAW2", "loc" -> "DALLAS",
      "calculated_field"->333, "another_calculated_field"->"A",
      "emp" -> scala.Array(Map("empno" -> null, "ename" -> "SMITH", "deptno" -> null,
          "deptno_name" -> List(Map("name" -> "20, RESEARCH (DALLAS)")),
          "work:empno"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null),
              Map("wdate"->"2012-7-10", "empno"->null, "hours"->8, "empno_mgr"->null))),
        Map("empno" -> null, "ename" -> "LEWIS", "deptno" -> null,
            "deptno_name" -> List(Map("name" -> "20, RESEARCH (DALLAS)")),
            "work:empno"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null)))))
    assertResult((List(1, List(List((List(1, List(List(1, 1))),10005), (List(1, List(List(1))),10006)))),10004))(
        ORT.insert("dept", obj))
    intercept[Exception](ORT.insert("no_table", obj))

    //insert with set parent id and do not insert existing tables with no link to parent
    //(work under dept)
    obj = Map("deptno" -> 50, "dname" -> "LAW3", "loc" -> "FLORIDA",
        "emp" -> List(Map("empno" -> null, "ename" -> "BROWN", "deptno" -> null),
          Map("empno" -> null, "ename" -> "CHRIS", "deptno" -> null)),
        "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null)))
    assertResult((List(1, List(List((1,10007), (1,10008)))),50))(ORT.insert("dept", obj))

    obj = Map("dname" -> "FOOTBALL", "loc" -> "MIAMI",
        "emp" -> List(Map("ename" -> "BROWN"), Map("ename" -> "CHRIS")))
    assertResult((List(1, List(List((1,10010), (1,10011)))),10009))(ORT.insert("dept", obj))

    obj = Map("ename" -> "KIKI", "deptno" -> 50, "car"-> List(Map("name"-> "GAZ")))
    assertResult((1,10012))(ORT.insert("emp", obj))

    //Ambiguous references to table: emp. Refs: List(Ref(List(empno)), Ref(List(empno_mgr)))
    obj = Map("emp" -> Map("empno" -> null, "ename" -> "BROWN", "deptno" -> null,
            "work"->List(Map("wdate"->"2012-7-9", "empno"->null, "hours"->8, "empno_mgr"->null))))
    intercept[Exception](ORT.insert("emp", obj))

    //child foreign key is also its primary key
    obj = Map("deptno" -> 60, "dname" -> "POLAR", "loc" -> "ALASKA",
              "dept_addr" -> List(Map("addr" -> "Halibut")))
    assertResult((List(1, List(List((1, 60)))),60))(ORT.insert("dept", obj))
    //child foreign key is also its primary key
    obj = Map("dname" -> "BEACH", "loc" -> "HAWAII",
              "dept_addr" -> List(Map("deptnr" -> 1, "addr" -> "Honolulu", "zip_code" -> "1010")))
    assertResult((List(1, List(List((1,10013)))),10013))(ORT.insert("dept", obj))

    obj = Map("deptno" -> null, "dname" -> "DRUGS",
              "car" -> List(Map("nr" -> "UUU", "name" -> "BEATLE")))
    assertResult((List(1, List(List((1, "UUU")))),10014))(ORT.insert("dept", obj))

    //multiple column primary key
    obj = Map("empno"->7788, "car_nr" -> "1111")
    assertResult(1)(ORT.insert("car_usage", obj))
    //primary key component not specified error must be thrown
    obj = Map("car_nr" -> "1111")
    intercept[SQLException](ORT.insert("car_usage", obj))
    obj = Map("date_from" -> "2013-10-24")
    intercept[SQLException](ORT.insert("car_usage", obj))
    obj = Map("empno" -> 7839)
    intercept[SQLException](ORT.insert("car_usage", obj))

    //value clause test
    obj = Map("car_nr" -> 2222, "empno" -> 7788, "date_from" -> "2013-11-06")
    assertResult(1)(ORT.insert("car_usage", obj))

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
        "work"->List(
            Map("wdate"->"2012-7-9", "empno"->7788, "hours"->8, "empno_mgr"->7839),
            Map("wdate"->"2012-7-10", "empno"->7788, "hours"->8, "empno_mgr"->7839)),
        "car" -> List(Map("nr" -> "EEE", "name" -> "BEATLE"), Map("nr" -> "III", "name" -> "FIAT")))
    assertResult(List(1,
        List(0,
            List((List(1, List(List(1, 1))),10015),
                (List(1, List(List())),10016)),
            0, List((1,"EEE"), (1,"III")))))(ORT.update("dept", obj))
    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work:empno"->List(
          Map("wdate"->"2012-7-9", "empno"->7788, "hours"->8, "empno_mgr"->7839),
          Map("wdate"->"2012-7-10", "empno"->7788, "hours"->8, "empno_mgr"->7839)),
        "calculated_children"->List(Map("x"->5)), "deptno"->40)
    assertResult(List(1, List(2, List(1, 1))))(ORT.update("emp", obj))
    //no child record is updated since no relation is found with car and ambiguous relation is
    //found with work
    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work"->List(Map("wdate"->"2012-7-9", "empno"->7788, "hours"->8, "empno_mgr"->7839),
              Map("wdate"->"2012-7-10", "empno"->7788, "hours"->8, "empno_mgr"->7839)),
        "calculated_children"->List(Map("x"->5)), "deptno"->40,
        "car"-> List(Map("nr" -> "AAA", "name"-> "GAZ", "deptno" -> 15)))
    assertResult(1)(ORT.update("emp", obj))

    //child foreign key is also its primary key (one to one relation)
    obj = Map("deptno" -> 60, "dname" -> "POLAR BEAR", "loc" -> "ALASKA",
              "dept_addr" -> List(Map("addr" -> "Halibut", "zip_code" -> "1010")))
    assertResult(List(1, List(List(1))))(ORT.update("dept", obj))

    //value clause test
    obj = Map("nr" -> 4444, "deptnr" -> 10)
    assertResult(1)(ORT.update("car", obj))
    obj = Map("nr" -> 4444, "deptnr" -> -1)
    intercept[java.sql.SQLIntegrityConstraintViolationException](ORT.update("car", obj))

    //update only children (no first level table column updates)
    obj = Map("nr" -> 4444, "tyres" -> List(Map("brand" -> "GOOD YEAR", "season" -> "S"),
        Map("brand" -> "PIRELLI", "season" -> "W")))
    assertResult(List(0, List((1,10017), (1,10018))))(ORT.update("car", obj))

    //delete children
    obj = Map("nr" -> 4444, "name" -> "LAMBORGHINI", "tyres" -> List())
    assertResult(List(1, List(2, List())))(ORT.update("car", obj))

    //update three level, for the first second level object it's third level is empty
    obj = Map("deptno" -> 10013, "emp" -> List(
        Map("ename" -> "ELKHADY",
            "work:empno" -> List()),
        Map("ename" -> "GUNTER",
            "work:empno" -> List(
            Map("wdate" -> "2014-08-27", "hours" -> 8),
            Map("wdate" -> "2014-08-28", "hours" -> 8)))))
    assertResult(List(0, List((List(1, List(List())), 10019), (List(1, List(List(1, 1))), 10020))))(
      ORT.update("dept", obj))
    //delete third level children
    obj = Map("deptno" -> 10013, "emp" -> List(
        Map("ename" -> "ELKHADY",
            "work:empno" -> List()),
        Map("ename" -> "GUNTER",
            "work:empno" -> List())))
    assertResult(List(2, List((List(1, List(List())), 10021), (List(1, List(List())), 10022))))(
      ORT.update("dept", obj))


    println("\n--- DELETE ---\n")

    assertResult(1)(ORT.delete("emp", 7934))

    println("\n--- SAVE - merge children ---\n")

    obj = Map("dname" -> "SALES", "loc" -> "WASHINGTON", "calculated_field" -> 222,
      "emp[+-=]" -> List(
        Map("empno" -> 7499, "ename" -> "ALLEN SMITH", "job" -> "SALESMAN", "mgr" -> 7698,
            "mgr_name" -> null, "deptno" -> 30),
        Map("empno" -> 7654, "ename" -> "MARTIN BLAKE", "job" -> "SALESMAN", "mgr" -> 7698,
            "mgr_name" -> null, "deptno" -> 30),
        Map("empno" -> null, "ename" -> "DEISE ROSE", "job" -> "SALESGIRL", "mgr" -> 7698,
            "mgr_name" -> null, "deptno" -> 30),
        Map("empno" -> 7698, "ename" -> "BLAKE", "job" -> "SALESMAN", "mgr" -> 7839,
            "mgr_name" -> null, "deptno" -> 30)),
      "calculated_children" -> List(Map("x" -> 5)), "deptno" -> 30)
      assertResult(List(1, List(3, List(1, 1, (1,10023), 1))))(ORT.update("dept", obj))

    obj = Map("empno" -> 7788, "ename"->"SCOTT", "mgr"-> 7839,
      "work:empno[+-=]" -> List(
        Map("wdate"->"2012-7-12", "empno"->7788, "hours"->10, "empno_mgr"->7839),
        Map("wdate"->"2012-7-13", "empno"->7788, "hours"->3, "empno_mgr"->7839)),
      "calculated_children"->List(Map("x"->5)), "deptno"->20)
    assertResult(List(1, List(2, List(1, 1))))(ORT.update("emp", obj))

    obj = Map("dname"->"DEVELOPMENT", "loc"->"DETROIT", "calculated_field"-> 222,
        "emp[+-=]"->List(
          Map("empno"->null, "ename"->"AMY", "mgr"->7788, "job"-> "SUPERVIS", "mgr_name"->null, "deptno"->40,
            "work:empno[+-=]"->List(
              Map("wdate"->"2012-7-12", "empno"->null, "hours"->5, "empno_mgr"->7839),
              Map("wdate"->"2012-7-13", "empno"->null, "hours"->2, "empno_mgr"->7839))),
          Map("empno"->null, "ename"->"LENE", "mgr"->7566, "job"-> "SUPERVIS", "mgr_name"->null, "deptno"->40,
            "work:empno[+-=]"->List(
              Map("wdate"->"2012-7-14", "empno"->null, "hours"->5, "empno_mgr"->7839),
              Map("wdate"->"2012-7-15", "empno"->null, "hours"->2, "empno_mgr"->7839)))),
        "calculated_children"->List(Map("x"->5)), "deptno"->40)
    assertResult(List(1, List(0, List((List(1, List(List(1, 1))),10024),
      (List(1, List(List(1, 1))),10025)))))(ORT.update("dept", obj))

    obj = Map("empno"->7788, "ename"->"SCOTT", "mgr"-> 7839,
        "work:empno[+-=]"->List(),
        "calculated_children"->List(Map("x"->5)), "deptno"->20)
    assertResult(List(1, List(2, List())))(ORT.update("emp", obj))

    println("\n---- Multiple table INSERT, UPDATE ------\n")

    obj = Map("dname" -> "SPORTS", "addr" -> "Brisbane", "zip_code" -> "4000")
    assertResult((List(1, List((1,10026))),10026))(ORT.insertMultiple(obj, "dept", "dept_addr")())

    obj = Map("deptno" -> 10026, "loc" -> "Brisbane", "addr" -> "Roma st. 150")
    assertResult(List(1, List(1)))(ORT.updateMultiple(obj, "dept", "dept_addr")())

    assertResult(List(Map(
        "dname" -> "SPORTS", "loc" -> "Brisbane",
        "addr" -> List(Map("addr" -> "Roma st. 150", "zip_code" -> "4000"))))) {
      tresql"dept[dname = 'SPORTS'] {dname, loc, |dept_addr {addr, zip_code} addr}".toListOfMaps
    }

    //update only first table in one to one relationship
    obj = Map("deptno" -> 60, "dname" -> "POLAR BEAR", "loc" -> "ALASKA")
    assertResult(1)(ORT.updateMultiple(obj, "dept", "dept_addr")())

    println("\n-------- EXTENDED CASES --------\n")

    //insert, update one to one relationship (pk of the extended table is fk for the base table) with children for the extended table
    obj = Map("dname" -> "PANDA BREEDING",
        "dept_addr" -> List(Map("addr" -> "Chengdu", "zip_code" -> "2000",
            "dept_sub_addr" -> List(Map("addr" -> "Jinli str. 10", "zip_code" -> "CN-1234"),
                Map("addr" -> "Jinjiang District", "zip_code" -> "CN-1234")))))
    assertResult((List(1, List(List((List(1, List(List(1, 1))),10027)))),10027)) {
      ORT.insert("dept", obj)}

    obj = Map("deptno" -> 10027, "dname" -> "PANDA BREEDING",
      "dept_addr" -> List(Map("addr" -> "Chengdu", "zip_code" -> "CN-1234",
        "dept_sub_addr" -> List(Map("addr" -> "Jinli str. 10", "zip_code" -> "CN-1234"),
          Map("addr" -> "Jinjiang District", "zip_code" -> "CN-1234")))))
    assertResult(List(1, List(List(List(1, List(2, List(1, 1))))))) {ORT.update("dept", obj)}

    println("\n-------- LOOKUP object editing --------\n")
    //edit lookup object
    obj = Map("brand" -> "DUNLOP", "season" -> "W", "carnr" -> Map("name" -> "VW"))
    assertResult(List(10028, (1,10029))) { ORT.insert("tyres", obj) }
    obj = Map("brand" -> "CONTINENTAL", "season" -> "W", "carnr" -> Map("nr" -> "UUU", "name" -> "VW"))
    assertResult(List("UUU", (1,10030))) { ORT.insert("tyres", obj) }
    obj = Map("nr" -> 10029, "season" -> "S", "carnr" -> Map("name" -> "SKODA"))
    assertResult(List(10031, 1)) { ORT.update("tyres", obj) }
    obj = Map("nr" -> 10029, "brand" -> "DUNLOP", "carnr" -> Map("nr" -> "UUU", "name" -> "VOLKSWAGEN"))
    assertResult(List("UUU", 1)) { ORT.update("tyres", obj) }
    //one to one relationship with lookup for extended table
    obj = Map("dname" -> "MARKETING", "addr" -> "Valkas str. 1",
        "zip_code" -> "LV-1010", "addr_nr" -> Map("addr" -> "Riga"))
    assertResult((List(1, List(List(10033, (1,10032)))),10032)) { ORT.insertMultiple(obj, "dept", "dept_addr")() }
    obj = Map("deptno" -> 10032, "dname" -> "MARKET", "addr" -> "Valkas str. 1a",
      "zip_code" -> "LV-1010", "addr_nr" -> Map("nr" -> 10033, "addr" -> "Riga, LV"))
    assertResult(List(1, List(List(10033, 1)))) { ORT.updateMultiple(obj, "dept", "dept_addr")() }
    obj = Map("deptno" -> 10032, "dname" -> "MARKETING", "addr" -> "Liela str.",
      "zip_code" -> "LV-1010", "addr_nr" -> Map("addr" -> "Saldus"))
    assertResult(List(1, List(List(10034, 1)))) { ORT.updateMultiple(obj, "dept", "dept_addr")() }
    //insert of lookup object where it's pk is present but null
    obj = Map("nr" -> 10029, "brand" -> "DUNLOP", "carnr" -> Map("nr" -> null, "name" -> "AUDI"))
    assertResult(List(10035, 1)) { ORT.update("tyres", obj) }

    println("\n----------------- Multiple table INSERT UPDATE extended cases ----------------------\n")

    obj = Map("dname" -> "AD", "ename" -> "Lee", "wdate" -> "2000-01-01", "hours" -> 3)
    assertResult((List(1, List((1,10036), (1,10036))),10036)) { ORT.insertMultiple(obj, "dept", "emp", "work:empno")() }

    obj = Map("deptno" -> 10036, "dname" -> "ADVERTISING", "ename" -> "Andy")
    assertResult(List(1, List(1))) { ORT.updateMultiple(obj, "dept", "emp")() }

    obj = Map("dname" -> "DIG", "emp" -> List(Map("ename" -> "O'Jay",
        "work:empno" -> List(Map("wdate" -> "2010-05-01", "hours" -> 7)))), "addr" -> "Tvaika 1")
    assertResult((List(1, List(List((List(1, List(List(1))),10038)), (1,10037))),10037)) {
      ORT.insertMultiple(obj, "dept", "dept_addr")() }

    obj = Map("dname" -> "GARAGE", "name" -> "Nissan", "brand" -> "Dunlop", "season" -> "S")
    assertResult((List(1, List((1,10039), (1,10039))),10039)) { ORT.insertMultiple(obj, "dept", "car", "tyres")() }

    obj = Map("deptno" -> 10039, "dname" -> "STOCK", "name" -> "Nissan", "brand" -> "PIRELLI", "season" -> "W")
    assertResult(List(1, List(1, 1))) { ORT.updateMultiple(obj, "dept", "car", "tyres")() }

    obj = Map("deptno" -> 10039, "dname" -> "STOCK", "name" -> "Nissan Patrol",
        "tyres" -> List(Map("brand" -> "CONTINENTAL", "season" -> "W")))
    assertResult(List(1, List(List(1, List(1, List((1,10040))))))) { ORT.updateMultiple(obj, "dept", "car")() }

    obj = Map("dname" -> "NEW STOCK", "name" -> "Audi Q7",
        "tyres" -> List(Map("brand" -> "NOKIAN", "season" -> "S")))
    assertResult((List(1, List((List(1, List(List((1,10042)))),10041))),10041)) { ORT.insertMultiple (obj, "dept", "car")() }

    println("\n-------- LOOKUP extended cases - chaining, children --------\n")

    obj = Map("brand" -> "Nokian", "season" -> "W", "carnr" ->
      Map("name" -> "Mercedes", "deptnr" -> Map("dname" -> "Logistics")))
    assertResult(List(10044, (1,10045))) { ORT.insert("tyres", obj) }

    obj = Map("nr" -> 10045, "brand" -> "Nokian", "season" -> "S", "carnr" ->
      Map("nr" -> 10044, "name" -> "Mercedes Benz", "deptnr" -> Map("deptno" -> 10043, "dname" -> "Logistics dept")))
    assertResult(List(10044, 1)) { ORT.update("tyres", obj) }

    obj = Map("dname" -> "MILITARY", "loc" -> "Alabama", "emp" -> List(
      Map("ename" -> "Selina", "mgr" -> null),
      Map("ename" -> "Vano", "mgr" -> Map("ename" -> "Carlo", "deptno" -> Map("dname" -> "Head")))))
    assertResult((List(1, List(List(List(null, (1,10047)), List(10049, (1,10050))))),10046)) { ORT.insert("dept", obj) }

    obj = Map("deptno" -> 10046, "dname" -> "METEO", "loc" -> "Texas", "emp" -> List(
      Map("ename" -> "Selina", "mgr" -> null),
      Map("ename" -> "Vano", "mgr" -> Map("ename" -> "Pedro", "deptno" -> Map("deptno" -> 10048, "dname" -> "Head")))))
    assertResult(List(1, List(2, List(List(null, (1,10051)), List(10052, (1,10053)))))) { ORT.update("dept", obj) }

    obj = Map("name" -> "Dodge", "car_usage" -> List(
      Map("empno" -> Map("empno" -> null, "ename" -> "Nicky", "job" -> "MGR", "deptno" -> 10)),
      Map("empno" -> Map("empno" -> 10052, "ename" -> "Lara", "job" -> "MGR", "deptno" -> 10))))
    assertResult((List(1, List(List(List(10055, 1), List(10052, 1)))),10054)) { ORT.insert("car", obj) }

    println("\n-------- INSERT, UPDATE with additional filter --------\n")
    //insert, update with additional filter
    assertResult(0){ORT.insert("dummy", Map("dummy" -> 2), "dummy = -1")}
    assertResult(1){ORT.insert("dummy d", Map("dummy" -> 2), "d.dummy = 2")}
    assertResult(0){ORT.update("address a", Map("nr" -> 10033, "addr" -> "gugu"), "a.addr ~ 'Ri'")}
    assertResult(0) { ORT.delete("emp e", 10053, "ename ~~ :ename", Map("ename" -> "ivans%")) }
    assertResult(1) { ORT.delete("emp e", 10053, "ename ~~ :ename", Map("ename" -> "van%")) }

    println("\n---- Object INSERT, UPDATE ------\n")

    implicit def pohatoMap[T <: Poha](o: T): (String, Map[String, _]) = o match {
      case Car(nr, name) => "car" -> Map("nr" -> nr, "name" -> name)
    }
    assertResult((1,8888))(ORT.insertObj(Car(8888, "OPEL")))
    assertResult(1)(ORT.updateObj(Car(8888, "SAAB")))

    println("\n-------- SAVE - extended cases --------\n")

    obj = Map("dname" -> "TRUCK DEPT",
      "car[+=]" -> List(
        Map("name" -> "VOLVO",
          "tyres[+=]" -> List(
            Map("brand" -> "BRIDGESTONE", "season" -> "S",
              "tyres_usage[+=]" -> List(
                Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-04-25"),
                Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-05-01"))),
            Map("brand" -> "COPARTNER", "season" -> "W",
              "tyres_usage[+=]" -> List(
                Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-09-25"),
                Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-10-01"))))
       ),
       Map("name" -> "TATA",
          "tyres[+=]" -> List(
           Map("brand" -> "METRO TYRE", "season" -> "S",
             "tyres_usage[+=]" -> List(
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2016-04-25"),
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2016-05-01"))),
           Map("brand" -> "GRL", "season" -> "W",
             "tyres_usage[+=]" -> List(
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2016-09-25"),
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2016-10-01")))))))
    assertResult((List(1, List(List((List(1, List(List((List(1,
      List(List(1, 1))),10058), (List(1, List(List(1, 1))),10059)))),10057),
      (List(1, List(List((List(1, List(List(1, 1))),10061),
      (List(1, List(List(1, 1))),10062)))),10060)))),10056))(ORT.insert("dept", obj))

    obj = Map("deptno" -> 10056, "dname" -> "TRUCK DEPT",
      "car[+=]" -> List(
        Map("nr" -> 10057, "name" -> "VOLVO",
          "tyres[+=]" -> List(
            Map("nr" -> 10058, "brand" -> "BRIDGESTONE", "season" -> "S",
              "tyres_usage[+=]" -> List(
                Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2016-04-01"))),
            Map("nr" -> null, "brand" -> "ADDO", "season" -> "W",
              "tyres_usage[+=]" -> List(
                Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2016-09-25"),
                Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2016-10-01"))))),
        Map("nr" -> 10060, "name" -> "TATA MOTORS",
          "tyres[+=]" -> List(
           Map("nr" -> 10061, "brand" -> "METRO TYRE", "season" -> "S",
             "tyres_usage[+=]" -> List(
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-04-25"),
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-05-01"))),
           Map("nr" -> 10062, "brand" -> "GRL", "season" -> "W",
             "tyres_usage[+=]" -> List(
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-09-25"),
               Map("carnr" -> null /*value expr is used from env*/, "date_from" -> "2015-10-01")))))))
      assertResult(List(1, List(List(List(1, List(List(List(1, List(List(1))),
        (List(1, List(List(1, 1))),10063)))), List(1, List(List(List(1, List(
          List(1, 1))), List(1, List(List(1, 1))))))))))(ORT.update("dept", obj))

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
        println(s"$nr. Testing tresql method of:\n$st")
        QueryParser.parseExp(st) match {
          case e: QueryParser.Exp => assert(e === QueryParser.parseExp(e.tresql))
        }
      case _ =>
    }

    println("\n-------------- CACHE -----------------\n")
    Env.cache map println

  }

}
