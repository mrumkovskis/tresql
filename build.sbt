val scalaV = "2.13.1"

lazy val commonSettings = Seq(
  organization := "org.tresql",
  scalaVersion := scalaV,
  crossScalaVersions := Seq(
      scalaV,
      "2.12.8",
      "2.11.8",
      "2.10.6"
    ),
  //coverageEnabled := true,
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-language:dynamics",
    "-language:postfixOps", "-language:implicitConversions", "-language:reflectiveCalls",
    "-language:existentials",
    "-Xmacro-settings:metadataFactoryClass=org.tresql.compiling.CompilerJDBCMetadata, driverClass=org.hsqldb.jdbc.JDBCDriver, url=jdbc:hsqldb:mem:., dbCreateScript=src/test/resources/db.sql, functionSignatures=org.tresql.test.TestFunctionSignatures"), //, verbose")
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
  publishArtifact in Compile := false,
  publishMavenStyle := true,
  sources in (Compile, doc) := Seq.empty
)

def coreDependencies(scalaVer: String) =
  Seq("org.scala-lang" % "scala-reflect" % scalaVer) ++
    ( if (scalaVer.startsWith("2.10.")) Nil else Seq("org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"))

lazy val core = (project in file("core"))
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .settings(
    name := "tresql-core",
    libraryDependencies ++= coreDependencies(scalaVersion.value),
    skip in publish := true,
  ).settings(commonSettings: _*)

lazy val macros = (project in file("macro"))
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .dependsOn(core)
  .settings(
    name := "macro",
    excludeFilter in unmanagedSources := (if (scalaVersion.value.startsWith("2.10.")) "*.*" else ""),
    skip in publish := true,
)
  .settings(commonSettings: _*)

val packageScopes = Seq(packageBin, packageSrc)

val packageProjects = Seq(core, macros)

val packageMerges = for {
  project <- packageProjects
  scope <- packageScopes
} yield mappings in(Compile, scope) := (mappings in (Compile, scope)).value ++ (mappings in (project, Compile, scope)).value


import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}

lazy val tresql = (project in file("."))
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .dependsOn(core % "test->test;compile->compile", macros)
  .aggregate(core, macros)
  .settings(
    //compiler macro works only on scala 2.12.x
    excludeFilter in (Test, unmanagedSources) := (if (!scalaVersion.value.startsWith("2.12.")) "CompilerMacroDependantTests.scala" else "")
  )
  .settings(commonSettings: _*)
  .settings(packageMerges: _*)
  .settings(
    sources in (Compile, doc) := (sources in (core, Compile)).value ++ (sources in (macros, Compile)).value,

    name := "tresql",
    libraryDependencies ++= coreDependencies(scalaVersion.value) ++
      Seq("org.scalatest" %% "scalatest" % "3.0.8" % "test,it",
        "org.hsqldb" % "hsqldb" % "2.3.1" % "test",
        "org.postgresql" % "postgresql" % "42.1.4" % "it,test"),
    initialCommands in console := "import org.tresql._; import org.tresql.implicits._; import org.scalatest._",
    publishArtifact in Test := false,
    publishArtifact in Compile := true,
    pomIncludeRepository := { x => false },
    pomPostProcess := { (node: XmlNode) =>
      new RuleTransformer(new RewriteRule {
        override def transform(node: XmlNode): XmlNodeSeq = node match {
          case e: Elem if e.label == "dependency" && e.child.exists(child => child.text == "org.tresql") =>
            val organization = e.child.filter(_.label == "groupId").flatMap(_.text).mkString
            val artifact = e.child.filter(_.label == "artifactId").flatMap(_.text).mkString
            val version = e.child.filter(_.label == "version").flatMap(_.text).mkString
            Comment(s"provided dependency $organization#$artifact;$version has been omitted")
          case _ => node
        }
      }).transform(node).head
    },
    pomExtra := <url>https://github.com/mrumkovskis/tresql</url>
      <licenses>
        <license>
          <name>MIT</name>
          <url>http://www.opensource.org/licenses/MIT</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:mrumkovskis/tresql.git</url>
        <connection>scm:git:git@github.com:mrumkovskis/tresql.git</connection>
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
  .configs(IntegrationTest extend(Test))
  .settings(Defaults.itSettings) 
