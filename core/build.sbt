name := "tresql"

organization := "org.tresql"

scalaVersion := "2.10.3"

crossScalaVersions := Seq("2.9.1", "2.9.2", "2.9.3", "2.10.3")

scalacOptions ++= Seq("-deprecation", "-Xexperimental")

scalacOptions <<= (scalaVersion, scalacOptions) map 
  {(v, o)=> if(v.startsWith("2.10")) o ++ Seq("-language:dynamics") else o}

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "1.9.1" % "test", 
                            "org.hsqldb" % "hsqldb" % "2.2.8" % "test")

publishTo <<= version { v: String =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("LOCAL"))
    Some( Resolver.file("Local", Path.userHome / ".m2" / "repository" asFile))
  else if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { x => false }

pomExtra := (
  <url>https://github.com/mrumkovskis/Query</url>
  <licenses>
    <license>
      <name>MIT</name>
      <url>http://www.opensource.org/licenses/MIT</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:mrumkovskis/Query.git</url>
    <connection>scm:git:git@github.com:mrumkovskis/Query.git</connection>
  </scm>
  <developers>
    <developer>
      <id>mrumkovskis</id>
      <name>Martins Rumkovskis</name>
      <url>https://github.com/mrumkovskis/</url>
    </developer>
    <developer>
      <id>guntiso</id>
      <name>Guntis Ozols</name>
      <url>https://github.com/guntiso/</url>
    </developer>
  </developers>
)
