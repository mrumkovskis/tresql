import sbt._

class QueryProject(info: ProjectInfo) extends ParentProject(info) {
  
  lazy val core = project("core", "Query Core", new QueryCoreProject(_))
  //lazy val service = project("service", "Query service", new QueryServicesProject(_), core)
  lazy val webapp = project("service", "Query web app", new LiftWebProject(_), core)
  
  //val unisorepo = "Uniso Repo" at "http://159.148.47.6:8087/artifactory/libs-release-local"

  //override def managedStyle = ManagedStyle.Maven
  //Credentials.add("Artifactory Realm", "159.148.47.6", "admin", "uniso123")
  //Credentials(Path.userHome / ".m2" / ".credentials", log)
  //val publishTo = unisorepo

}

protected class QueryCoreProject(info: ProjectInfo) extends DefaultProject(info) {
  override def libraryDependencies = Set(
      "org.scalatest" % "scalatest_2.9.0-1" % "1.6.1" % "test",
      "org.hsqldb" % "hsqldb-j5" % "2.2.4" % "test") ++ super.libraryDependencies
}

protected class QueryServicesProject(info: ProjectInfo) extends DefaultProject(info) {
  val liftVersion = "2.4-M3"
  lazy val JavaNet = "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
  override def libraryDependencies = Set(
    "net.liftweb" %% "lift-webkit" % liftVersion % "compile",
    "net.liftweb" %% "lift-mapper" % liftVersion % "compile",
    "org.mortbay.jetty" % "jetty" % "6.1.26" % "test",
    "junit" % "junit" % "4.7" % "test",
    "ch.qos.logback" % "logback-classic" % "0.9.26",
    "org.scala-tools.testing" %% "specs" % "1.6.8" % "test",
    "com.h2database" % "h2" % "1.2.147") ++ super.libraryDependencies
}


protected class LiftWebProject(info: ProjectInfo) extends DefaultWebProject(info) {
  val liftVersion = "2.4-M3"

  // uncomment the following if you want to use the snapshot repo
  //  val scalatoolsSnapshot = ScalaToolsSnapshots

  // If you're using JRebel for Lift development, uncomment
  // this line
  // override def scanDirectories = Nil
    
  val myPath = "src"/"main"/"webapp"/"WEB-INF"/"jetty-web.xml"
  val myPathAsJavaFileName = myPath.asFile
  
  val fileContent = new myJettyWebXml {
	val jndiName = "java:comp/env/" + systemOptional[String]("uniso.query.jndi.name","jdbc/uniso/query").value 
	val driverClassName = systemOptional[String]("uniso.query.driver.class","org.postgresql.Driver").value
	val url = systemOptional[String]("uniso.query.url","jdbc:postgresql://x.x.x.x/database").value
	val user = systemOptional[String]("uniso.query.user","userName").value
	val password = systemOptional[String]("uniso.query.password","password").value
  }
	   
  lazy val preProcessToCreateJettyWebXml = task {
	//FileUtilities.clean(myPath, log)
    //FileUtilities.append(myPathAsJavaFileName, fileContent.toXML.toString, log)
	if (!myPathAsJavaFileName.exists) FileUtilities.append(myPathAsJavaFileName, fileContent.toXML.toString, log)
	None
  }
  
  override def compileAction = super.compileAction dependsOn(preProcessToCreateJettyWebXml)  
  
  
  lazy val JavaNet = "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"

  override def libraryDependencies = Set(
    "net.liftweb" %% "lift-webkit" % liftVersion % "compile",
    "net.liftweb" %% "lift-mapper" % liftVersion % "compile",
    "org.mortbay.jetty" % "jetty" % "6.1.26" % "test",
    "org.mortbay.jetty" % "jetty-plus" % "6.1.26", 		//for jetty to work with jndi
    "org.mortbay.jetty" % "jetty-naming" % "6.1.26", 	//for jetty to work with jndi
    "commons-dbcp" % "commons-dbcp" % "1.4",			//for example "org.postgresql.ds.PGSimpleDataSource" don't provide variable url (to avoid for need to use other variables in sbt file, this package is added)
    "junit" % "junit" % "4.7" % "test",
    "ch.qos.logback" % "logback-classic" % "0.9.26",
    "org.scala-tools.testing" %% "specs" % "1.6.8" % "test",
    "com.h2database" % "h2" % "1.2.147") ++ super.libraryDependencies
}

abstract class myJettyWebXml {
  val jndiName:String
  val driverClassName: String
  val url:String
  val user:String
  val password: String
  
  def toXML = 
<Configure class="org.mortbay.jetty.webapp.WebAppContext">
  <New id="Test" class="org.mortbay.jetty.plus.naming.Resource">
    <Arg></Arg>
    <Arg>{jndiName}</Arg>
    <Arg>
      <New class="org.apache.commons.dbcp.BasicDataSource">
        <Set name="url">{url}</Set>
        <Set name="username">{user}</Set>
        <Set name="password">{password}</Set>
        <Set name="driverClassName">{driverClassName}</Set>
      </New>
    </Arg>
  </New>
</Configure>   
}


