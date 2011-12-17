import sbt._
import Keys._

object TreSQLBuild extends Build {

  lazy val core = Project(id = "core", base = file("core"))

  lazy val service = Project(id = "service", base = file("service")) dependsOn (core)

  //lazy val webapp = Project(id = "webapp", base = file("service")) dependsOn ()

  //this is necessary for service un webapp projects.
  //NOTE: in sbt file setting must be initialized in order to be used, like for example liftVersion := "1.0" 
  val liftVersion = SettingKey[String]("lift-version", "Lift version used for service build")

}