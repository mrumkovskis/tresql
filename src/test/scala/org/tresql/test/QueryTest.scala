package org.tresql.test

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import java.sql.DriverManager
import org.tresql._
import org.tresql.implicits._
import org.tresql.result.Jsonizer._
import scala.util.parsing.json._

import sys._

/** To run from console {{{org.scalatest.run(new test.QueryTest)}}} */
class QueryTest extends FunSuite with BeforeAndAfterAll {
  class TestFunctions extends Functions {
    def echo(x: String) = x
    def plus(a: java.lang.Long, b: java.lang.Long) = a + b
    def average(a: BigDecimal, b: BigDecimal) = (a + b) / 2
    def dept_desc(d: String, ec: String) = d + " (" + ec + ")"
    def nopars() = "ok"
    import CoreTypes._
    def dept_count(implicit res: Resources) = Query("dept{count(*)}")(res).unique[Int]
    def dept_desc_with_empc(d: String)(implicit res: Resources) =
      d + " emp count - " + Query("emp[deptno = (dept[dname = ?]{deptno})]{count(*)}", d)(res).unique[String]
    def vararg_with_resources(s: String*)(implicit res: Resources) =
      s.mkString("", ", ", " - ") + Query("dummy{count(dummy)}")(res).unique[String]
  }
  object Macros extends org.tresql.Macros {
    import macro_._
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
    def dummy(b: QueryBuilder) = b.buildExpr("dummy")
    def macro_interpolator_test1(implicit b: QueryBuilder, e1: Expr, e2: Expr) = macro_"($e1 + $e2)"
  }

  val executeCompilerMacroDependantTests = scala.util.Properties.versionNumberString.startsWith("2.12.")
  val compilerMacroDependantTests =
    if (executeCompilerMacroDependantTests)
      Class.forName("org.tresql.test.CompilerMacroDependantTests").newInstance
        .asInstanceOf[CompilerMacroDependantTestsApi]
    else
      null

  override def beforeAll {
    //initialize environment
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    val conn = DriverManager.getConnection("jdbc:hsqldb:mem:.")
    Env.conn = conn
    Env.dialect = dialects.HSQLDialect orElse {
      case e: QueryBuilder#SelectExpr =>
        val b = e.builder
        e match {
          case s @ b.SelectExpr(List(b.Table(b.ConstExpr(null), _, _, _, _)), _, _, _, _, _, _, _, _, _) =>
            s.copy(tables = List(s.tables.head.copy(table = b.IdentExpr(List("dummy"))))).sql
          case _ => e.defaultSQL
        }
    }
    Env.idExpr = s => "nextval('seq')"
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
  }

  test("tresql statements") {
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
    println("\n---------------- Test TreSQL statements ----------------------\n")
    testTresqls("/test.txt", (st, params, patternRes, nr) => {
      println(s"Executing test #$nr:")
      val testRes = jsonize(if (params == null) Query(st) else Query(st, parsePars(params)), Arrays)
      println("Result: " + testRes)
      assert(JSON.parseFull(testRes).get === JSON.parseFull(patternRes).get)
    })
  }

  if (executeCompilerMacroDependantTests) test("API") {
    compilerMacroDependantTests.api
  }

  if (executeCompilerMacroDependantTests) test("ORT") {
    compilerMacroDependantTests.ort
  }

  test("tresql methods") {
    println("\n---- TEST tresql methods of QueryParser.Exp ------\n")
    testTresqls("/test.txt", (tresql, _, _, nr) => {
      println(s"$nr. Testing tresql method of:\n$tresql")
      QueryParser.parseExp(tresql) match {
        case e: QueryParser.Exp @unchecked => assert(e === QueryParser.parseExp(e.tresql))
      }
    })
  }

  test("compiler") {
    println("\n-------------- TEST compiler ----------------\n")
    trait TestFunctionSignatures extends compiling.Functions {
      def macro_interpolator_test1(exp1: Any, exp2: Any): Any
    }
    //set new metadata
    Env.metaData = new metadata.JDBCMetaData with compiling.CompilerFunctions {
      override def compilerFunctions = classOf[TestFunctionSignatures]
    }
    testTresqls("/test.txt", (tresql, _, _, nr) => {
      println(s"$nr. Compiling tresql:\n$tresql")
      try QueryCompiler.compile(tresql)
      catch {
        case e: Exception => throw new RuntimeException(s"Error compiling statement #$nr:\n$tresql", e)
      }
    })

    //with recursive compile with braces select def
    QueryCompiler.compile("""t(*) {(emp[ename ~~ 'kin%']{empno}) +
      (t[t.empno = e.mgr]emp e{e.empno})} t#(1)""")

    //with recursive in column clause compile
    QueryCompiler.compile("""dummy { (kd(nr, name) {
      emp[ename ~~ 'kin%']{empno, ename} + emp[mgr = nr]kd{empno, ename}
    } kd {name}) val}""")

    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("work/dept{*}"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("works"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("emp{aa}"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("(dummy() ++ dummy()){dummy}"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("(dummy ++ dummiz())"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("(dummiz() ++ dummy){dummy}"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("dept/emp[l = 'x']{loc l}(l)^(count(loc) > 1)#(l || 'x')"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("dept/emp{loc l}(l)^(count(l) > 1)#(l || 'x')"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("dept d{deptno dn, dname || ' ' || loc}#(~(dept[dname = dn]{deptno}))"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("dept d[d.dname in (d[1]{dname})]"))
    intercept[QueryCompiler.CompilerException](QueryCompiler.compile("(dummy{dummy} + dummy{dummy d}) d{d}"))
  }

  if (executeCompilerMacroDependantTests) test("compiler macro") {
    compilerMacroDependantTests.compilerMacro
  }

  test("cache") {
    Env.cache map(c => println(s"\nCache size: ${c.size}\n"))
  }

  test("Test Java API") {
    Class.forName("org.tresql.test.TresqlJavaApiTest").newInstance.asInstanceOf[Runnable].run
  }

  def testTresqls(resource: String, testFunction: (String, String, String, Int) => Unit) = {
    var nr = 0
    new scala.io.BufferedSource(getClass.getResourceAsStream(resource))("UTF-8")
      .getLines.foreach {
        case l if (l.trim.startsWith("//")) =>
        case l if (l.trim.length > 0) =>
          val (st, params, patternRes) = l.split("-->") match {
            case scala.Array(s, r) => (s, null, r)
            case scala.Array(s, p, r) => (s, p, r)
          }
          nr += 1
          testFunction(st, params, patternRes, nr)
        case _ =>
      }
  }
}
