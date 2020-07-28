package org.tresql.test

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.sql.{Connection, DriverManager, ResultSet}

import org.tresql._
import org.tresql.metadata.JDBCMetadata
import org.tresql.result.Jsonizer._

import scala.util.parsing.json._
import sys._

/** To run from console {{{org.scalatest.run(new test.QueryTest)}}} */
class QueryTest extends FunSuite with BeforeAndAfterAll {
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
    case c: QueryBuilder#CastExpr => c.exp.sql
  }

  var tresqlResources: Resources = null

  override def beforeAll = {
    //initialize environment
    Class.forName("org.hsqldb.jdbc.JDBCDriver")
    val conn = DriverManager.getConnection("jdbc:hsqldb:mem:.")
    tresqlResources = new Resources {}
      .withMetadata(JDBCMetadata(conn))
      .withConn(conn)
      .withDialect(hsqlDialect)
      .withIdExpr(_ => "nextval('seq')")
      .withMacros(Macros)
      .withCache(new SimpleCache(-1))
      .withLogger((msg, _, topic) => if (topic != LogTopic.sql_with_params) println (msg))
    //create test db script
    new scala.io.BufferedSource(getClass.getResourceAsStream("/db.sql")).mkString.split("//").foreach {
      sql => val st = conn.createStatement; tresqlResources.log("Creating database:\n" + sql); st.execute(sql); st.close
    }
    //set resources for console
    ConsoleResources.resources = tresqlResources
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
    implicit val testResources = tresqlResources
    testTresqls("/test.txt", (st, params, patternRes, nr) => {
      println(s"Executing test #$nr:")
      val testRes = jsonize(if (params == null) Query(st) else Query(st, parsePars(params)), Arrays)
      println("Result: " + testRes)
      assert(JSON.parseFull(testRes).get === JSON.parseFull(patternRes).get)
    })
  }

  if (executeCompilerMacroDependantTests) test("API") {
    compilerMacroDependantTests.api(tresqlResources)
  }

  if (executeCompilerMacroDependantTests) test("ORT") {
    compilerMacroDependantTests.ort(tresqlResources)
  }

  test("tresql methods") {
    implicit val testRes = tresqlResources
    println("\n---- TEST tresql methods of QueryParser.Exp ------\n")
    val parser = new QueryParser(testRes, testRes.cache)
    testTresqls("/test.txt", (tresql, _, _, nr) => {
      println(s"$nr. Testing tresql method of:\n$tresql")
      parser.parseExp(tresql) match {
        case e: parsing.Exp @unchecked => assert(e === parser.parseExp(e.tresql))
      }
    })
  }

  test("compiler") {
    implicit val testRes = tresqlResources.withMetadata(
      new metadata.JDBCMetadata with compiling.CompilerFunctionMetadata {
        override def conn: Connection = tresqlResources.conn
        override def compilerFunctionSignatures = classOf[org.tresql.test.TestFunctionSignatures]
      }
    )
    println("\n-------------- TEST compiler ----------------\n")
    val compiler = new QueryCompiler(testRes.metadata, testRes)
    import compiling.CompilerException
    testTresqls("/test.txt", (tresql, _, _, nr) => {
      println(s"$nr. Compiling tresql:\n$tresql")
      try compile(tresql)
      catch {
        case e: Exception => throw new RuntimeException(s"Error compiling statement #$nr:\n$tresql", e)
      }
    })

    def compile(exp: String) =
      try compiler.compile(exp) catch {
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
    intercept[CompilerException](compiler.compile("=dept_addr da [da.addr_nr = a.nr] (address a {a.addr}) a {da.addr = a.addr}"))
    intercept[CompilerException](compiler.compile("=dept_addr da [da.addr_nr = nrz] (address a {a.nr, a.addr}) a {da.addr = a.addr}"))
    intercept[CompilerException](compiler.compile("=dept_addr da [da.addr_nr = a.nr] address a {addr = addr}"))
    intercept[CompilerException](compiler.compile("=dept_addr da [da.addr_nr = a.nr] address a {1}"))

    intercept[CompilerException](compiler.compile("work/dept{*}"))
    intercept[CompilerException](compiler.compile("works"))
    intercept[CompilerException](compiler.compile("emp{aa}"))
    intercept[CompilerException](compiler.compile("(dummy() ++ dummy()){dummy}"))
    intercept[CompilerException](compiler.compile("(dummy ++ dummiz())"))
    intercept[CompilerException](compiler.compile("(dummiz() ++ dummy){dummy}"))
    intercept[CompilerException](compiler.compile("dept/emp[l = 'x']{loc l}(l)^(count(loc) > 1)#(l || 'x')"))
    intercept[CompilerException](compiler.compile("dept/emp{loc l}(l)^(count(l) > 1)#(l || 'x')"))
    intercept[CompilerException](compiler.compile("dept d{deptno dn, dname || ' ' || loc}#(~(dept[dname = dn]{deptno}))"))
    intercept[CompilerException](compiler.compile("dept d[d.dname in (d[1]{dname})]"))
    intercept[CompilerException](compiler.compile("(dummy{dummy} + dummy{dummy d}) d{d}"))
    intercept[CompilerException](compiler.compile("dept{group_concat(dname)#(dnamez)}"))
    intercept[CompilerException](compiler.compile("dept{group_concat(dname)#(dname)[dept{deptnox} < 30]}"))
    intercept[CompilerException](compiler.compile("dept{group_concat(dname)#(dname)[deptno{deptno} < 30]}"))
    intercept[CompilerException](compiler.compile("{dept[10]{dnamez}}"))
    intercept[CompilerException](compiler.compile("b(# y) {a{x}}, a(# x) {dummy{dummy}} b{y}"))
    intercept[CompilerException](compiler.compile("i(# ename){emp e[empno = '']{*}} i{*}"))
    intercept[CompilerException](compiler.compile("d(# id) { dummy[dummy =0] }, u(# id) {dummy[dummy =2]}, upd(#) {dummy[dummy in (u.id)]{dummy} = [u.id + 1] }, remove_from(# ) { dummy - [ dummy in (d{id}) ] } remove_from{dummy}"))
    intercept[CompilerException](compiler.compile("d(# id) { dummy[dummy =0] }, u(# id) {dummy[dummy =2]}, upd(#) {dummy[dummy in (u.id)]{dummy} = [u.id + 1] }, remove_from(# ) { dummy - [ dummy in (d{id}) ] } upd{dummy}"))
    intercept[CompilerException](compiler.compile("d1(# *) { dummy a[]dummy b { b.dummy col }}, d2(# *) { d1[]dummy? {dummy.dummy d, d1.col c} }, i(# *) { +dummy {dummy} d1[col = 1]{col} {dummy} }, u(# *) { =dummy[dummy = d2.c]d2 {dummy = d2.c} {d2.c u, d1.col } } u"))
    intercept[CompilerException](compiler.compile("d(# dname) {dept{dname}} =dept[d.dname = 'x']{deptno, dname} d{#dept, dname || '[reorganized]'}"))
    intercept[CompilerException](compiler.compile("[]dummy_table() d(d){d.x}"))
    intercept[CompilerException](compiler.compile("macro_interpolator_test4(dname, dept)"))
    intercept[CompilerException](compiler.compile("macro_interpolator_test4(dept, name)"))
    intercept[CompilerException](compiler.compile("dept [dname = 'RESEARCH'] {dname, if_defined(:x, macro_interpolator_test(:x, 2))}"))
    intercept[CompilerException](compiler.compile("e(*){emp{ename}}, d(*) {dept{dname}}"))

    //insert with asterisk column
    intercept[CompilerException](compiler.compile("+dummy{*} dummy{dummy kiza}"))

    //two aliases
    intercept[Exception](compiler.compile("dummy {dummy a b}"))
  }

  if (executeCompilerMacroDependantTests) test("compiler macro") {
    compilerMacroDependantTests.compilerMacro(tresqlResources)
  }

  test("dialects") {
    println("\n------------------ Test dialects -----------------\n")
    implicit val testRes = tresqlResources.withDialect(dialects.PostgresqlDialect)
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
      "with d(dname) as (select dname from dept) update dept set deptno = t_.deptno, dname = t_.dname from (select ? as deptno, dname || '[reorganized]' as dname from d) t_ where d.dname = 'x'"
    )
    assertResult(Query.build("d(# dname) {dept{dname}} =dept[dept.dname = d.dname]d[d.dname = 'x'] {deptno = #dept, dname = d.dname}").sql)(
      "with d(dname) as (select dname from dept) update dept set deptno = ?, dname = d.dname from d where (dept.dname = d.dname) and (d.dname = 'x')"
    )
    assertResult(Query.build("d(# dname) {dept[deptno = 1]{dname}} dept - [deptno in d{deptno}]").sql)(
      "with d(dname) as (select dname from dept where deptno = 1) delete from dept where deptno in (select deptno from d)"
    )
  }

  test("cache") {
    Option(tresqlResources.cache) foreach(c => println(s"\nCache size: ${c.size}\n"))
  }

  test("Test Java API") {
    val res = new java_api.ThreadLocalResources {
      override def cache: Cache = tresqlResources.cache
    }
    res.conn = tresqlResources.conn
    res.dialect = tresqlResources.dialect
    res.idExpr = tresqlResources.idExpr
    res.metadata = tresqlResources.metadata
    Class.forName("org.tresql.test.TresqlJavaApiTest").getDeclaredConstructor().newInstance()
      .asInstanceOf[org.tresql.test.TresqlJavaApiTest].run(res)
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

object ConsoleResources {
  //used in console
  implicit var resources: Resources = _
}
