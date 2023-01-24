package org.tresql.test

import java.io.ByteArrayOutputStream
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Connection, DriverManager}
import org.tresql._
import org.tresql.metadata.{JDBCMetadata, TypeMapper}

import scala.util.control.NonFatal
import sys._

/** To run from console {{{new org.tresql.test.PGQueryTest().execute(configMap = ConfigMap("docker" -> "postgres", "remove" -> "false"))}}},
  * to run from sbt - {{{it:testOnly * -- -oD -Ddocker=<docker image name> [-Dport=<posgtres host port>] [-Dwait_after_startup_millis=<wait time after postgres docker start until connection port is bound>] [-Dremove=<true|false - whether to stop docker after test are run, useful in console mode>]}}},
  * example
  * 1. specific postgres version - {{{it:testOnly * -- -oD -Ddocker=postgres:10.2}}}
  * 2. latest postgres version and do not remove postgres container after test run with specific postgres host port
  *    and wait time after docker started until jdbc connection attempt is made -
  *   {{{it:testOnly * -- -oD -Ddocker=postgres -Dremove=false -Dport=54321 -Dwait_after_startup_millis=4000}}} */
class PGQueryTest extends AnyFunSuite with BeforeAndAfterAllConfigMap {
  val executePGCompilerMacroDependantTests =
    !scala.util.Properties.versionNumberString.startsWith("2.10") &&
    !scala.util.Properties.versionNumberString.startsWith("2.11")

  val PGcompilerMacroDependantTests =
    if (executePGCompilerMacroDependantTests)
      Class.forName("org.tresql.test.PGCompilerMacroDependantTests").newInstance
        .asInstanceOf[PGCompilerMacroDependantTestsApi]
    else
      null

  var tresqlResources: Resources = null

  trait PGTypeMapper extends TypeMapper {
    override def to_sql_type(vendor: String, typeName: String): String = {
      if (vendor == "postgresql") {
        Map(
          "string" -> "text",
          "short" -> "smallint",
          "int" -> "integer",
          "long" -> "bigint",
          "integer" -> "numeric",
          "float" -> "float",
          "double" -> "double precision",
          "decimal" -> "numeric",
          "boolean" -> "bool",
          "date" -> "date",
          "time" -> "time",
          "dateTime" -> "timestamp",
          "bytes" -> "bytea",
        ).getOrElse(typeName, typeName)
      } else super.to_sql_type(vendor, typeName)
    }
  }

  override def beforeAll(configMap: ConfigMap) = {
    //initialize environment
    Class.forName("org.postgresql.Driver")
    val jdbcPort = configMap.getOptional[String]("port").map(":" + _).getOrElse("")
    val (dbUri, dbUser, dbPwd) = (s"jdbc:postgresql://localhost$jdbcPort/tresql", "tresql", "tresql")
    val connection = if (configMap.get("docker").isDefined) {
      val postgresDockerImage = configMap("docker")
      val hostPort = configMap.getOrElse("port", "5432")
      val DockerCmd =
        s"""docker run -d --rm --name tresql-it-tests -p $hostPort:5432
           | -e POSTGRES_DB=tresql -e POSTGRES_USER=tresql -e POSTGRES_PASSWORD=tresql -e POSTGRES_HOST_AUTH_METHOD=trust
           | $postgresDockerImage""".stripMargin
      println(s"Starting tresql test docker postgres container...")
      val process = Runtime.getRuntime.exec(DockerCmd)
      val baos = new ByteArrayOutputStream()
      val errStream = process.getErrorStream
      var i: Int = errStream.read
      while (i != -1) {
        i = errStream.read
        baos.write(i)
      }
      process.waitFor
      val errTip =
        """Try to specify different test parameters as postgres port: -Dport=<port>
          |  or increase docker startup wait time -Dwait_after_startup_millis
          |For complete parameter list see scaladoc of PGQueryTest class.""".stripMargin
      val exitValue = process.exitValue
      if (exitValue != 0) {
        println(s"Error occured during executing command:\n$DockerCmd")
        println(baos.toString("utf8"))
        println()
        println(errTip)
        println()
        sys.error("Failure")
      } else {
        println("Docker started.")
        val timeout = configMap.getOptional[String]("wait_after_startup_millis").map(_.toLong).getOrElse(2000L)
        println(s"Wait $timeout milliseconds for db port binding")
        Thread.sleep(timeout)
        try DriverManager.getConnection(dbUri, dbUser, dbPwd)
        catch {
          case NonFatal(e) =>
            sys.error(s"Error occurred trying to connect to database ($dbUri, $dbUser, $dbPwd) - ${e.toString}.\n" + errTip)
        }
      }
    } else try DriverManager.getConnection(dbUri, dbUser, dbPwd) catch {
      case e: Exception =>
        throw sys.error(s"Unable to connect to database: ${e.toString}.\n" +
          "For postgres docker container try command: it:testOnly * -- -oD -Ddocker=postgres -Dport=<port> -Dwait_after_startup_millis=<time to wait for postgres for startup>")
    }
    val md = new JDBCMetadata with PGTypeMapper {
      override def conn: Connection = connection
    }
    tresqlResources = new Resources {}
      .withConn(connection)
      .withMetadata(md)
      .withDialect(dialects.PostgresqlDialect orElse dialects.VariableNameDialect)
      .withIdExpr(_ => "nextval('seq')")
      .withMacros(Macros)
      .withCache(new SimpleCache(-1))
      .withLogger((msg, _, topic) => if (topic != LogTopic.sql_with_params) println (msg))
    //create test db script
    new scala.io.BufferedSource(getClass.getResourceAsStream("/pgdb.sql")).mkString.split("//").foreach {
      sql => val st = connection.createStatement; tresqlResources.log("Creating database:\n" + sql); st.execute(sql); st.close
    }
    //set resources for console
    ITConsoleResources.resources = tresqlResources
  }

  override def afterAll(configMap: ConfigMap) = {
    if (configMap.contains("docker") &&
      !configMap.get("remove").filter(_ == "false").isDefined) {
      val DockerCmd = "docker stop tresql-it-tests"
      print(s"Stopping tresql test docker postgres container...")
      val process = Runtime.getRuntime.exec(DockerCmd)
      val baos = new ByteArrayOutputStream()
      val errStream = process.getErrorStream
      var i: Int = errStream.read
      while (i != -1) {
        i = errStream.read
        baos.write(i)
      }
      process.waitFor
      val exitValue = process.exitValue
      if (exitValue != 0) {
        println(s"Error occured during executing command:\n$DockerCmd")
        println(baos.toString("utf8"))
      } else {
        print(baos.toString("utf8"))
        println("docker stopped.")
      }
    }
  }

  test("tresql statements") {
    implicit val testResources = tresqlResources
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
        case A(_, ac) =>
          if (ac.isEmpty) List()
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
    import io.bullet.borer.Json
    import io.bullet.borer.Dom._
    import TresqlResultBorerElementTranscoder._

    testTresqls("/pgtest.txt", (st, params, patternRes, nr) => {
      val pattern = jsonDomToAny(Json.decode(patternRes.getBytes("UTF8")).to[Element].value)
      assertResult(pattern, st) {
        jsonDomToAny(resultToJsonDom(if (params == null) Query(st) else Query(st, parsePars(params))))
      }
    })
  }

  if (executePGCompilerMacroDependantTests) test("PG API") {
    PGcompilerMacroDependantTests.api(tresqlResources)
  }

  if (executePGCompilerMacroDependantTests) test("PG ORT") {
    PGcompilerMacroDependantTests.ort(tresqlResources)
  }

  test("tresql methods") {
    implicit val testRes = tresqlResources
    println("\n---- TEST tresql methods of QueryParser.Exp ------\n")
    val parser = new QueryParser(testRes, testRes.cache)
    testTresqls("/pgtest.txt", (tresql, _, _, nr) => {
      println(s"$nr. Testing tresql method of:\n$tresql")
      parser.parseExp(tresql) match {
        case e: ast.Exp @unchecked => assert(e === parser.parseExp(e.tresql))
      }
    })
  }

  test("compiler") {
    println("\n-------------- TEST compiler ----------------\n")
    //set new metadata
    val testRes = tresqlResources.withMetadata(
      new JDBCMetadata with PGTypeMapper {
        override def conn: java.sql.Connection = tresqlResources.conn
        override def macrosClass: Class[_] = classOf[org.tresql.test.Macros]
      }
    )
    val compiler = new QueryCompiler(testRes.metadata, Map(), testRes)
    //set console compiler so it can be used from scala console
    ITConsoleResources.compiler = compiler
    import ast.CompilerException
    testTresqls("/pgtest.txt", (tresql, _, _, nr) => {
      println(s"$nr. Compiling tresql:\n$tresql")
      try compiler.compile(tresql)
      catch {
        case e: Exception => throw new RuntimeException(s"Error compiling statement #$nr:\n$tresql", e)
      }
    })

    //with recursive compile with braces select def
    compiler.compile("""t(*) {(emp[ename ~~ 'kin%']{empno}) +
      (t[t.empno = e.mgr]emp e{e.empno})} t#(1)""")

    //with recursive in column clause compile
    compiler.compile("""dummy { (kd(nr, name) {
      emp[ename ~~ 'kin%']{empno, ename} + emp[mgr = nr]kd{empno, ename}
    } kd {name}) val}""")

    //values from select compilation
    compiler.compile("=dept_addr da [da.addr_nr = a.nr] address a {da.addr = a.addr}")
    compiler.compile("=dept_addr da [da.addr_nr = address.nr] address {da.addr = address.addr}")
    compiler.compile("=dept_addr da [da.addr_nr = a.nr] (address a {a.nr, a.addr}) a {da.addr = a.addr}")
    compiler.compile("=dept_addr da [da.addr_nr = nr] (address a {a.nr, a.addr}) a {da.addr = a.addr}")

    //postgresql style cast compilation
    compiler.compile("dummy[dummy::int = 1 & dummy::'double precision' & (dummy + dummy)::long] {dummy::int}")

    //values from select compilation errors
    intercept[CompilerException](compiler.compile("=dept_addr da [da.addr_nr = a.nr] (address a {a.addr}) a {da.addr = a.addr}"))
    intercept[CompilerException](compiler.compile("=dept_addr da [da.addr_nr = nrz] (address a {a.nr, a.addr}) a {da.addr = a.addr}"))

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
  }

  if (executePGCompilerMacroDependantTests) test("postgres compiler macro") {
    PGcompilerMacroDependantTests.compilerMacro(tresqlResources)
  }

  test("ast serialization") {
    import io.bullet.borer._
    import io.bullet.borer.derivation.MapBasedCodecs._
    import io.bullet.borer.Codec
    import org.tresql.ast._
    import CompilerAst._
    implicit val exc: Codec[CompilerExp] = Codec(Encoder { (w, _) => w.writeNull() }, Decoder { r => r.readNull() })
    implicit val tableColDefCodec: Codec[TableColDef] = deriveCodec[TableColDef]
    implicit lazy val expCodec: Codec[Exp] = deriveAllCodecs[Exp]
    val p = new QueryParser()
    testTresqls("/pgtest.txt", (st, _, _, nr) => {
      val e = p.parseExp(st)
      val ev = try Cbor.encode(e).toByteArray catch {
        case e: Exception => throw new RuntimeException(s"Error encoding statement nr. $nr:\n$st", e)
      }
      assertResult(e, st) {
        Cbor.decode(ev).to[Exp].value
      }
    })
  }

  test("cache") {
    Option(tresqlResources.cache) map(c => println(s"\nCache size: ${c.size}\n"))
  }

  def testTresqls(resource: String, testFunction: (String, String, String, Int) => Unit) = {
    var nr = 0
    new scala.io.BufferedSource(getClass.getResourceAsStream(resource))("UTF-8")
      .getLines().foreach {
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

object ITConsoleResources {
  //used in console
  implicit var resources: Resources = _
  var compiler: QueryCompiler = _
}
