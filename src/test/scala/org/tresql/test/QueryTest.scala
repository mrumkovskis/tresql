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
        .map(v => b.VarExpr(v.replace("?", ""), Nil, v endsWith "?"))
      val sqlSnippet = varRegex.replaceAllIn(value, " ?")
      if (vars.exists(v => v.opt && !(b.env contains v.name)))
        b.SQLExpr("null", Nil)
      else b.SQLExpr(sqlSnippet, vars)
    }
    def in_twice(implicit b: QueryBuilder, expr: Expr, in: Expr) = macro_"$expr in ($in, $in)"
    def null_macros(b: QueryBuilder) = null
    def dummy(b: QueryBuilder) = b.buildExpr("dummy")
    def dummy_table(b: QueryBuilder) = b.IdentExpr(List("dummy"))
    def macro_interpolator_test1(implicit b: QueryBuilder, e1: Expr, e2: Expr) = macro_"($e1 + $e2)"
    def macro_interpolator_test2(implicit b: QueryBuilder, e1: Expr, e2: Expr) =
      macro_"(macro_interpolator_test1($e1, $e1) + macro_interpolator_test1($e2, $e2))"
    def macro_interpolator_test3(implicit b: QueryBuilder, e1: Expr, e2: Expr) =
      macro_"(macro_interpolator_test2($e1 * $e1, $e2 * $e2))"
  }

  val executeCompilerMacroDependantTests =
    !scala.util.Properties.versionNumberString.startsWith("2.10") &&
    !scala.util.Properties.versionNumberString.startsWith("2.11")


  val compilerMacroDependantTests =
    if (executeCompilerMacroDependantTests)
      Class.forName("org.tresql.test.CompilerMacroDependantTests").getDeclaredConstructor().newInstance()
        .asInstanceOf[CompilerMacroDependantTestsApi]
    else
      null

  val hsqlDialect: CoreTypes.Dialect = dialects.HSQLDialect orElse dialects.VariableNameDialect orElse {
    case e: QueryBuilder#SelectExpr =>
      val b = e.builder
      e match {
        case s @ b.SelectExpr(List(b.Table(b.ConstExpr(QueryParser.Null), _, _, _, _)), _, _, _, _, _, _, _, _, _) =>
          s.copy(tables = List(s.tables.head.copy(table = b.IdentExpr(List("dummy"))))).sql
        case _ => e.defaultSQL
      }
    case c: QueryBuilder#CastExpr => c.exp.sql
  }

  override def beforeAll {
    //initialize environment
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    val conn = DriverManager.getConnection("jdbc:hsqldb:mem:.")
    Env.conn = conn
    Env.dialect = hsqlDialect
    Env.idExpr = s => "nextval('seq')"
    Env.macros = Macros
    Env.cache = new SimpleCache(-1)
    Env.logger = (msg, _, topic) => if (topic != LogTopic.sql_with_params) println (msg)
    Env updateValueExprs /*value expr*/ Map(("car_usage" -> "empno") -> "(emp[empno = :empno]{empno})",
      ("car" -> "deptnr") -> "(case((dept[deptno = :deptnr] {count(deptno)}) = 1, (dept[deptno = :deptnr] {deptno}), -1))",
      ("tyres_usage" -> "carnr") -> ":#car")
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
        case s if s.startsWith("'") => s.substring(1, s.length)
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
    //set new metadata
    Env.metadata = new metadata.JDBCMetadata with compiling.CompilerFunctionMetadata {
      override def compilerFunctionSignatures = classOf[org.tresql.test.TestFunctionSignatures]
    }
    import QueryCompiler.{compile => qcompile, CompilerException}
    testTresqls("/test.txt", (tresql, _, _, nr) => {
      println(s"$nr. Compiling tresql:\n$tresql")
      try compile(tresql)
      catch {
        case e: Exception => throw new RuntimeException(s"Error compiling statement #$nr:\n$tresql", e)
      }
    })

    def compile(exp: String) =
      try qcompile(exp) catch {
        case e: Exception => throw new RuntimeException(s"Error compiling statement $exp", e)
      }

    //with recursive compile with braces select def
    compile("""t(*) {(emp[ename ~~ 'kin%']{empno}) +
      (t[t.empno = e.mgr]emp e{e.empno})} t#(1)""")

    //with recursive in column clause compile
    compile("""dummy { (kd(nr, name) {
      emp[ename ~~ 'kin%']{empno, ename} + emp[mgr = nr]kd{empno, ename}
    } kd {name}) val}""")

    //with expression with asterisk resolving
    compile("i(# *){emp e[empno = '']{*}} i{*}")
    compile("i(# *){emp e[empno = '']{e.*}} i{*}")
    compile("i(*){emp e[empno = '']{e.*} + i[false]emp e{e.*}} i{*}")
    compile("e(# *){emp{empno, ename}}, t(# *){i(*){e[empno = '']{e.*} + i[false]e{e.*}}i{*}}t{*}")
    compile("dept{(i(){emp e{ename} + i[false]emp e{i.ename}} i{ename}) x}")
    compile("dept{(i(*){emp e{ename} + i[false]emp e{i.ename}} i{ename}) x}")

    //with expression with dml statement
    compile("d(# dname) {dept{dname}} +dept{deptno, dname} d{#dept, dname || '[reorganized]'}")
    compile("d(# dname) {dept{dname}} =dept[dept.dname = 'x']{deptno, dname} d{#dept, dname || '[reorganized]'}")
    compile("d(# dname) {dept{dname}} =dept[dept.dname = d.dname]d[d.dname = 'x'] {deptno = #dept, dept.dname = d.dname}")
    compile("d(# dname) {dept[deptno = 1]{dname}} dept - [deptno in d{deptno}]")

    //values from select compilation
    compile("=dept_addr da [da.addr_nr = a.nr] address a {da.addr = a.addr}")
    compile("=dept_addr da [da.addr_nr = a.nr] address a {addr = a.addr}")
    compile("=dept_addr da [da.addr_nr = a.nr] address a {addr = da.addr}")
    compile("=dept_addr da [da.addr_nr = address.nr] address {da.addr = address.addr}")
    compile("=dept_addr da [da.addr_nr = a.nr] (address a {a.nr, a.addr}) a {da.addr = a.addr}")
    compile("=dept_addr da [da.addr_nr = nr] (address a {a.nr, a.addr}) a {da.addr = a.addr}")
    compile("=dept_addr da [da.addr_nr = nr] (address a {a.nr, a.addr}) a {da.addr = 'ADDR'}")
    compile("=dept_addr da / address a / dept_addr da1 {da.addr = a.nr}")

    //postgresql style cast compilation
    compile("dummy[dummy::int = 1 & dummy::'double precision' & (dummy + dummy)::long] {dummy::int}")

    //returing compilation
    compile("i(#) { +dummy{dummy} [:v] {*} }, u(#) {=dummy [dummy = :v] {dummy = :v + 1} {*}}, d (#) {dummy - [dummy = :v] {*}} i ++ u ++ d")
    compile("d1(# *) { dummy a[]dummy b { b.dummy col }}, d2(# *) { d1[]dummy? {dummy.dummy d, d1.col c} }, i(# *) { +dummy {dummy} d1[col = 1]{col} {dummy} }, u(# *) { =dummy[dummy = d2.c]d2 {dummy = d2.c} {d2.c u } } u")

    //values from select compilation errors
    intercept[CompilerException](qcompile("=dept_addr da [da.addr_nr = a.nr] (address a {a.addr}) a {da.addr = a.addr}"))
    intercept[CompilerException](qcompile("=dept_addr da [da.addr_nr = nrz] (address a {a.nr, a.addr}) a {da.addr = a.addr}"))
    intercept[CompilerException](qcompile("=dept_addr da [da.addr_nr = a.nr] address a {addr = addr}"))
    intercept[CompilerException](qcompile("=dept_addr da [da.addr_nr = a.nr] address a {1}"))

    intercept[CompilerException](qcompile("work/dept{*}"))
    intercept[CompilerException](qcompile("works"))
    intercept[CompilerException](qcompile("emp{aa}"))
    intercept[CompilerException](qcompile("(dummy() ++ dummy()){dummy}"))
    intercept[CompilerException](qcompile("(dummy ++ dummiz())"))
    intercept[CompilerException](qcompile("(dummiz() ++ dummy){dummy}"))
    intercept[CompilerException](qcompile("dept/emp[l = 'x']{loc l}(l)^(count(loc) > 1)#(l || 'x')"))
    intercept[CompilerException](qcompile("dept/emp{loc l}(l)^(count(l) > 1)#(l || 'x')"))
    intercept[CompilerException](qcompile("dept d{deptno dn, dname || ' ' || loc}#(~(dept[dname = dn]{deptno}))"))
    intercept[CompilerException](qcompile("dept d[d.dname in (d[1]{dname})]"))
    intercept[CompilerException](qcompile("(dummy{dummy} + dummy{dummy d}) d{d}"))
    intercept[CompilerException](qcompile("dept{group_concat(dname)#(dnamez)}"))
    intercept[CompilerException](qcompile("dept{group_concat(dname)#(dname)[dept{deptnox} < 30]}"))
    intercept[CompilerException](qcompile("dept{group_concat(dname)#(dname)[deptno{deptno} < 30]}"))
    intercept[CompilerException](qcompile("{dept[10]{dnamez}}"))
    intercept[CompilerException](qcompile("b(# y) {a{x}}, a(# x) {dummy{dummy}} b{y}"))
    intercept[CompilerException](qcompile("i(# ename){emp e[empno = '']{*}} i{*}"))
    intercept[CompilerException](qcompile("d(# id) { dummy[dummy =0] }, u(# id) {dummy[dummy =2]}, upd(#) {dummy[dummy in (u.id)]{dummy} = [u.id + 1] }, remove_from(# ) { dummy - [ dummy in (d{id}) ] } remove_from{dummy}"))
    intercept[CompilerException](qcompile("d(# id) { dummy[dummy =0] }, u(# id) {dummy[dummy =2]}, upd(#) {dummy[dummy in (u.id)]{dummy} = [u.id + 1] }, remove_from(# ) { dummy - [ dummy in (d{id}) ] } upd{dummy}"))
    intercept[CompilerException](qcompile("d1(# *) { dummy a[]dummy b { b.dummy col }}, d2(# *) { d1[]dummy? {dummy.dummy d, d1.col c} }, i(# *) { +dummy {dummy} d1[col = 1]{col} {dummy} }, u(# *) { =dummy[dummy = d2.c]d2 {dummy = d2.c} {d2.c u, d1.col } } u"))
    intercept[CompilerException](qcompile("d(# dname) {dept{dname}} =dept[d.dname = 'x']{deptno, dname} d{#dept, dname || '[reorganized]'}"))
    intercept[CompilerException](qcompile("[]dummy_table() d(d){d.x}"))

    //insert with asterisk column
    intercept[CompilerException](qcompile("+dummy{*} dummy{dummy kiza}"))

    //two aliases
    intercept[Exception](qcompile("dummy {dummy a b}"))
  }

  if (executeCompilerMacroDependantTests) test("compiler macro") {
    compilerMacroDependantTests.compilerMacro
  }

  test("dialects") {
    println("\n------------------ Test dialects -----------------\n")
    Env.dialect = dialects.PostgresqlDialect
    assertResult(Query.build("a::'b'").sql)("select * from a::b")
    assertResult(Query.build("a {1::int + 2::'double precision'}").sql)("select 1::int + 2::double precision from a")
    assertResult(Query.build("=dept_addr da [da.addr_nr = a.nr] (addr a {a.addr}) a {da.addr = a.addr}").sql)(
      "update dept_addr da set da.addr = a.addr from (select a.addr from addr a) a where da.addr_nr = a.nr")
    assertResult(Query.build("=dept_addr da [da.addr_nr = a.nr] addr a {da.addr = a.addr}").sql)(
      "update dept_addr da set da.addr = a.addr from addr a where da.addr_nr = a.nr")
    assertResult(Query.build("=dept_addr da [da.addr_nr = a.nr] (addr a {a.addr}) a [da.addr_nr = 1 & a.addr = 'a'] {da.addr = a.addr}").sql)(
      "update dept_addr da set da.addr = a.addr from (select a.addr from addr a) a where (da.addr_nr = a.nr) and (da.addr_nr = 1 and a.addr = 'a')")
    assertResult(Query.build("=dept_addr da [da.addr_nr = a.nr] addr a [da.addr_nr = 1 & a.addr = 'a'] {da.addr = a.addr}").sql)(
      "update dept_addr da set da.addr = a.addr from addr a where (da.addr_nr = a.nr) and (da.addr_nr = 1 and a.addr = 'a')")
    assertResult(Query.build("=dept_addr da / address a / dept_addr da1 {da.addr = 'ADDR'}").sql)(
      "update dept_addr da set da.addr = 'ADDR' from address a left join dept_addr da1 on a.nr = da1.addr_nr where da.addr_nr = a.nr"
    )
    assertResult(Query.build("d(# dname) {dept{dname}} +dept{deptno, dname} d{#dept, dname || '[reorganized]'}").sql)(
      "with d(dname) as (select dname from dept) insert into dept (deptno, dname) select ?, dname || '[reorganized]' from d")
    assertResult(Query.build("d(# dname) {dept{dname}} =dept[d.dname = 'x']{deptno, dname} d{#dept, dname || '[reorganized]'}").sql)(
      "with d(dname) as (select dname from dept) update dept set (deptno, dname) = (select ?, dname || '[reorganized]' from d) where d.dname = 'x'"
    )
    assertResult(Query.build("d(# dname) {dept{dname}} =dept[dept.dname = d.dname]d[d.dname = 'x'] {deptno = #dept, dname = d.dname}").sql)(
      "with d(dname) as (select dname from dept) update dept set deptno = ?, dname = d.dname from d where (dept.dname = d.dname) and (d.dname = 'x')"
    )
    assertResult(Query.build("d(# dname) {dept[deptno = 1]{dname}} dept - [deptno in d{deptno}]").sql)(
      "with d(dname) as (select dname from dept where deptno = 1) delete from dept where deptno in (select deptno from d)"
    )

    //restore hsql dialect
    Env.dialect = hsqlDialect
  }

  test("cache") {
    Env.cache foreach(c => println(s"\nCache size: ${c.size}\n"))
  }

  test("Test Java API") {
    Class.forName("org.tresql.test.TresqlJavaApiTest").getDeclaredConstructor().newInstance()
      .asInstanceOf[Runnable].run
  }

  def testTresqls(resource: String, testFunction: (String, String, String, Int) => Unit) = {
    var nr = 0
    new scala.io.BufferedSource(getClass.getResourceAsStream(resource))("UTF-8")
      .getLines.foreach {
        case l if l.trim.startsWith("//") =>
        case l if l.trim.length > 0 =>
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
