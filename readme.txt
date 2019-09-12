Test console with hsqldb env
============================

sbt test:console
run { new test.QueryTest }

Query("emp")
tresql"emp[job='SALESMAN']{*}#(ename)".toList.map(_.ename) foreach println
tresql"emp[ename='SCOTT']{*}".toListOfMaps


Postgres test env docker setup
==============================

see src/it/scala/PGQueryTest.scala
