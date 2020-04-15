Installing and usage
==============================

### Dependency in your project
To use tresql, add the module to your project

For sbt project add to your build.sbt file - `libraryDependencies += "org.tresql" %% "tresql" % "9.3.2"`

### Try tresql using sbt console with test database

1. Clone tresql project from github `git clone https://github.com/mrumkovskis/tresql.git && cd tresql`
2. Start sbt `sbt test:console`
3. Run tests to create in memory [hsqldb](http://hsqldb.org) database with test data `run (new test.QueryTest)`
4. Try your tresql statements like `tresql"dept[dname = 'RESEARCH']".toListOfMaps`
5. Database structure see [db.sql](/mrumkovskis/tresql/blob/develop/src/test/resources/db.sql)

### Try using postgres test database
_Prerequisite - you need docker to be installed on your system._

1. Clone tresql project from github `git clone https://github.com/mrumkovskis/tresql.git && cd tresql`
2. Start sbt `sbt it:console`
3. Run tests to create [postgresql](https://www.postgresql.org) database with test data
   `new org.tresql.test.PGQueryTest().execute(configMap = ConfigMap("docker" -> "postgres:10.2", "remove" -> "false"))`.
   Parameter `"remove" -> false` prevents docker from dropping postgres container after tests are executed.

   > NOTE: after exiting console postgres docker container can be removed with `docker stop tresql-it-tests`.
   If running of tests fails due to connection error. Stop container and rerun tests with increased waiting time for docker startup like
   `new org.tresql.test.PGQueryTest().execute(configMap = ConfigMap("docker" -> "postgres:10.2", "remove" -> "false", "wait_after_startup_millis" -> "5000"))`
   Or if default postgres port is busy specify other like `"port" -> "54321"`
4. Try your tresql statements like `tresql"dept[dname = 'RESEARCH']".toListOfMaps`
5. Database structure see [pgdb.sql](/mrumkovskis/tresql/blob/develop/src/it/resources/pgdb.sql)
