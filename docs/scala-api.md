Using TreSQL from Scala
=======================

[Summary](#summary)  
[Sample script](#sample)  
[Language Guide](language-guide)

For using TreSQL from Scala, you should use a jar from classes compiled under /core directory.

First, you get a db connection. (This is not really a concern of TreSQL, but TreSQL provides some shortcuts for you to obtain java.sql.Connection as well).
Next, you execute an SQL statement and (for selects) iterate through the results. The (simplified) architecture for that is made up of following steps:

Query string -> Parsing -> Building expression -> Execution -> Result

1. You supply a query string
2. This query string is parsed
3. An internal expression is build from parsed string that can be execeuted
4. You execute this expression, optionally supplying binding parameters
5. You obtain a Result and iterate through it.

Here's a usage example (examples use data described in [Appendix](language-guide#wiki-appendix-data) to [Language Guide](language-guide)): 

Set up environment variables for connection (using Postgres db for example):

```
set JAVA_OPTS=-Djdbc.drivers=org.postgresql.Driver -Duniso.query.db=jdbc:postgresql://yourHost/yourDB -Duniso.query.user=dbUser -Duniso.query.password=dbPassword

scala %JAVA_OPTS% -classpath lib/postgres_jdbc.jar;lib/query_core.jar test.scala
```

Starting test.scala: 

```scala
Class.forName("org.postgresql.Driver")
import uniso.query._
import uniso.query.result._

val md = metadata.JDBCMetaData("yourDB", "public")
val conn = Conn()()
```
This is a shortcut way of obtaining java.sql.Connection into conn.

Env class holds database connection and metadata. Set it up:

```scala
Env update md                              // stores metadata
Env update conn                            // stores connection
Env update ((msg, level) => println (msg)) // sample set up for logger function
```

Now, you can fire TreSQL statements right away:

```scala
Query("""emp [ ename = "KING"] { sal } = [5001]""")
```
This executes UPDATE statement to set KING's salary to 5001. This way, you do parsing, building and execution all in one step.

You can accomplish the same with unnamed binding variables. Here, you supply binding variable values as parameters to Query(s, ...) (variable number of parameters). (The values could also be supplied as List).

```scala
val s = """emp [ ename = ?] { sal } = [?]"""
Query(s, "KING", 5001)
```

Same with named binding variables (supply values in a Map):

```scala
val s = """emp [ ename = :ename ] { sal } = [ :sal ]"""
Query(s, Map("ename" -> "KING", "sal" -> 5001)
```

If you want to reuse the statement for another set of binding variables, you should first build expression and then execute it:

```scala
val s = """emp [ ename = :ename ] { sal } = [ :sal ]"""
val stmt = Query.build(s)

var params = Map("ename" -> "KING", "sal" -> 5001)
stmt(params)
params = Map("ename" -> "BLAKE", "sal" -> 3001)
stmt(params)
```

Use the following example to select and iterate through results:

```scala
val s = """emp [job = :job] { ename, job }"""
val stmt = Query.build(s)
val params = Map("job" -> "SALESMAN")

val rs = stmt(params).asInstanceOf[Result].toList
for (row <- rs) {
  val content = row.content
  val (ename, job) = (content(0), content(1))
    println(String.format("%s, %s\n", ename.toString, job.toString))
}
```

Use Jsonizer class to receive results as JSON object:

```scala
val writer = new java.io.CharArrayWriter
Jsonizer.jsonize(stmt(params).asInstanceOf[Result], writer)
println(writer)
```

### <a name="wiki-summary"/> Summary

```scala
Env update md                              // stores metadata
Env update conn                            // stores connection
Env update ((msg, level) => println (msg)) // sample set up for logger function

Query(s)                       // execute statement 
Query(s, value1, value2, ...)  // with binding values
Query(s, List[Any])            // binding values as List
Query(s, Map [String, Any])    // binding values as Map

stmt = Query.build(String)     // build Expr
stmt(Map [String, Any])        // execute Expr with binding values as Map

rs = stmt(params).asInstanceOf[Result].toList  // get execution Result with rows
```

### <a name="wiki-sample"/> Sample script

```scala
Class.forName("org.postgresql.Driver")
import uniso.query._
import uniso.query.result._
val md = metadata.JDBCMetaData("querytest", "public")
val conn = Conn()()

Env update md
Env update conn
Env update ((msg, level) => println (msg))

println("----------start----------")
println(Env.conn)

// -------------SELECTs-----------------------------

var s = """emp [job = :job] { ename, job }"""
val stmt = Query.build(s)
val params = Map("job" -> "SALESMAN")

val rs = stmt(params).asInstanceOf[Result].toList
for (row <- rs) {
  val content = row.content
  val (ename, job) = (content(0), content(1))
    println(String.format("%s, %s\n", ename.toString, job.toString))
}

// -------------results as JSON---------------------
val writer = new java.io.CharArrayWriter
Jsonizer.jsonize(stmt(params).asInstanceOf[Result], writer)
println(writer)

s = """emp [ename = ?]"""
Jsonizer.jsonize(Query(s, "TURNER").asInstanceOf[Result], writer)
println(writer)

// ------------------UPDATE--------------------------
Query("""emp [ ename = "KING"] { sal } = [5001]""")

s = """emp [ ename = :ename ] { sal } = [ :sal ]"""
Query(s, Map("ename" -> "KING", "sal" -> 5001))

s = """emp [ ename = ?] { sal } = [?]"""
Query(s, "KING", 5001)

println("----------end----------")
```