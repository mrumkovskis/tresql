val paradiseVersion = "2.1.0"

lazy val commonSettings = Seq(
  organization := "org.tresql",
  scalaVersion := "2.11.8",
  //crossScalaVersions := Seq(2.10.4", "2.11.8")
  //coverageEnabled := true
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-language:dynamics",
    "-language:postfixOps", "-language:implicitConversions"),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
  addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full),
  publishArtifact in Compile := false,
  publishMavenStyle := true,
  sources in (Compile, doc) := Seq.empty
)

lazy val core = (project in file("core"))
  .settings(
    name := "tresql-core",
    libraryDependencies ++= Seq("org.scala-lang" % "scala-reflect" % scalaVersion.value),
    libraryDependencies := {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, scalaMajor)) if scalaMajor >= 11 =>
          libraryDependencies.value :+ "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
        case _ =>
          libraryDependencies.value
      }
    })
  .settings(commonSettings: _*)

lazy val macros = (project in file("macro"))
  .dependsOn(core)
  .settings(
    name := "macro"
  )
  .settings(commonSettings: _*)

val packageScopes = Seq(packageBin, packageSrc)

val packageProjects = Seq(core, macros)

val packageMerges = for {
  project <- packageProjects
  scope <- packageScopes
} yield mappings in(Compile, scope) := (mappings in (Compile, scope)).value ++ (mappings in (project, Compile, scope)).value


lazy val tresql = (project in file("."))
  .dependsOn(core, macros)
  .aggregate(core, macros)
  .settings(commonSettings: _*)
  .settings(packageMerges: _*)
  .settings(
    sources in (Compile, doc) := (sources in (core, Compile)).value ++ (sources in (macros, Compile)).value,

    name := "tresql",
    unmanagedSources in Test <<= (scalaVersion, unmanagedSources in Test) map {
      (v, d) => (if (v.startsWith("2.10")) d else d filterNot (_.getPath endsWith ".java")).get
    },
    libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "2.1.5" % "test",
                                "org.hsqldb" % "hsqldb" % "2.2.8" % "test"),
    initialCommands in console := "import org.tresql._",
    publishArtifact in Test := false,
    publishArtifact in Compile := true,
    pomIncludeRepository := { x => false },
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
  )
