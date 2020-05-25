tresql API guide
===================

Contents
--------
* [Query](#query)  
* [Result](#result)
* [Environment](#envirnonment)
    * [Tresql macro](#tresql-macro)  
* [Scala compiler macro](#scala-compiler-macro)  
* [ORT - Object relational transformations](#ort---object-relational-transformations)


Query 
--------------
TODO

Result
--------------
TODO

Envirnonment
--------------

TODO

### Tresql macro

TODO

Scala compiler macro
--------------------

Tresql provides tresql string interpolator implementation as scala compiler macro.
This technique enables, that tresql statement is also class definition with fields
corresponding result columns.

For example tresql in fragment below not only translates into sql and retrieves result
but is also a class definition with two fields `dname: String` and `loc: String`:

```scala
tresql"dept {dname, loc}".map(r => r.dname -> r.loc).toList: List[(String, String)]
```

Scala compiler tresql macro settings for tresql project (see [build.sbt](/build.sbt)):

```scala
  scalacOptions += 
    "-Xmacro-settings:metadataFactoryClass=org.tresql.compiling.CompilerJDBCMetadata, driverClass=org.hsqldb.jdbc.JDBCDriver, url=jdbc:hsqldb:mem:., dbCreateScript=src/test/resources/db.sql, functionSignatures=org.tresql.test.TestFunctionSignatures"
```

ORT - Object relational transformations
---------------------------------------
TODO