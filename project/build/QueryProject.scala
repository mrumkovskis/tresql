import sbt._

class QueryProject(info: ProjectInfo) extends ParentProject(info)
{
   lazy val core = project("core", "Query Core", new QueryCoreProject(_))
   lazy val service = project("service", "Query web service", new LiftProject(_), core)
      
}

protected class QueryCoreProject(info: ProjectInfo) extends DefaultProject(info) {
      override def libraryDependencies = Set("org.scalatest" % "scalatest_2.9.0-1" % "1.6.1" % "test",
           "org.hsqldb" % "hsqldb-j5" % "2.2.4" % "test") ++ super.libraryDependencies 
}

protected
class LiftProject(info: ProjectInfo) extends DefaultWebProject(info) {
  val liftVersion = "2.4-M3"

  // uncomment the following if you want to use the snapshot repo
  //  val scalatoolsSnapshot = ScalaToolsSnapshots

  // If you're using JRebel for Lift development, uncomment
  // this line
  // override def scanDirectories = Nil

  lazy val JavaNet = "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"

  override def libraryDependencies = Set(
    "net.liftweb" %% "lift-webkit" % liftVersion % "compile",
    "net.liftweb" %% "lift-mapper" % liftVersion % "compile",
    "org.mortbay.jetty" % "jetty" % "6.1.26" % "test",
    "junit" % "junit" % "4.7" % "test",
    "ch.qos.logback" % "logback-classic" % "0.9.26",
    "org.scala-tools.testing" %% "specs" % "1.6.8" % "test",
    "com.h2database" % "h2" % "1.2.147"
  ) ++ super.libraryDependencies
}
