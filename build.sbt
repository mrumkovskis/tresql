val scalaV = "2.13.10"

lazy val commonSettings = Seq(
  organization := "org.tresql",
  scalaVersion := scalaV,
  crossScalaVersions := Seq(
      scalaV,
      "2.12.17",
//      "3.2.2",
    ),
  //coverageEnabled := true,
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-language:dynamics",
    "-language:postfixOps", "-language:implicitConversions", "-language:reflectiveCalls",
    "-language:existentials",
  ),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
  Compile / publishArtifact := false,
  publishMavenStyle := true,
  Compile / doc / sources := Seq.empty
)

ThisBuild / versionScheme := Some("early-semver")

def coreDependencies(scalaVer: String) =
  Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
  ) ++ (if (scalaVer.startsWith("3")) Nil else Seq("org.scala-lang" % "scala-reflect" % scalaVer))

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
initialize := {
  val _ = initialize.value
  val javaVersion = sys.props("java.specification.version")
  if (javaVersion != "1.8")
    sys.error("Java 1.8 is required for this project. Found " + javaVersion + " instead")
}

lazy val core = (project in file("core"))
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .settings(
    name := "tresql-core",
    libraryDependencies ++= coreDependencies(scalaVersion.value),
    publish / skip := true,
  ).settings(commonSettings: _*)

lazy val macros = (project in file("macro"))
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .dependsOn(core)
  .settings(
    name := "tresql-interpolator-macro",
    publish / skip := true,
)
  .settings(commonSettings: _*)

val packageScopes = Seq(packageBin, packageSrc)

val packageProjects = Seq(core, macros)

val packageMerges = for {
  project <- packageProjects
  scope <- packageScopes
} yield Compile / scope / mappings := (Compile / scope / mappings).value ++ (project / Compile / scope / mappings).value


import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}

lazy val tresql = (project in file("."))
  .disablePlugins(plugins.JUnitXmlReportPlugin)
  .dependsOn(core % "test->test;compile->compile", macros)
  .aggregate(core, macros)
  //.settings(scalacOptions += "-Xmacro-settings:verbose")
  .settings(commonSettings: _*)
  .settings(packageMerges: _*)
  .settings(
    Compile / doc / sources := (core / Compile / sources).value ++ (macros / Compile / sources).value,

    name := "tresql",
    libraryDependencies ++= coreDependencies(scalaVersion.value) ++ {
      val borerV    = scalaVersion.value match {
        case v if v startsWith "2.12" => "1.7.2"
        case v if v startsWith "2.13" => "1.8.0"
        case v if v startsWith "3"    => "1.10.2"
      }
      Seq("org.scalatest" %% "scalatest" % "3.2.15" % "test,it",
        ("org.hsqldb" % "hsqldb" % "2.7.1" % "test").classifier("jdk8"),
        "io.bullet"     %% "borer-core"       % borerV    % "test,it",
        "io.bullet"     %% "borer-derivation" % borerV    % "test,it",
        "org.postgresql" % "postgresql"       % "42.5.4"  % "it,test",
      )
    },
    Test / console / initialCommands := "import org.tresql._; import org.scalatest._; import org.tresql.test.ConsoleResources._",
    IntegrationTest / console / initialCommands := "import org.tresql._; import org.scalatest._; import org.tresql.test.ITConsoleResources._",
    Test / publishArtifact := false,
    Compile / publishArtifact := true,
    pomIncludeRepository := { _ => false },
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
