name := "tresql"

organization := "org.tresql"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

//coverageEnabled := true

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-language:dynamics",
  "-language:postfixOps", "-language:implicitConversions")

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "2.1.5" % "test",
                            "org.hsqldb" % "hsqldb" % "2.2.8" % "test")

libraryDependencies := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      libraryDependencies.value :+ "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2"
    case _ =>
      libraryDependencies.value
  }
}

unmanagedSources in Test <<= (scalaVersion, unmanagedSources in Test) map {
  (v, d) => (if (v.startsWith("2.10")) d else d filterNot (_.getPath endsWith ".java")).get
}

initialCommands in console := "import org.tresql._"

publishTo <<= version { v: String =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { x => false }

pomExtra := (
  <url>https://github.com/uniso/tresql</url>
  <licenses>
    <license>
      <name>MIT</name>
      <url>http://www.opensource.org/licenses/MIT</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:uniso/tresql.git</url>
    <connection>scm:git:git@github.com:uniso/tresql.git</connection>
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
