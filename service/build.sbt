name := "tresql-web-service"

organization := "org"

version := "1.0"

scalaVersion := "2.9.1"

scalacOptions += "-deprecation"

liftVersion := "2.4-M4"

seq(webSettings :_*)

env in Compile := Some(file(".") / "conf" / "jetty" / "jetty-env.xml" asFile)

libraryDependencies <++= liftVersion {liftVersion=>Seq(
    "net.liftweb" %% "lift-webkit" % liftVersion % "compile",
    "net.liftweb" %% "lift-mapper" % liftVersion % "compile",
    "org.mortbay.jetty" % "jetty" % "6.1.26" % "container",
    "org.mortbay.jetty" % "jetty-plus" % "6.1.26" % "container",
    "org.mortbay.jetty" % "jetty-naming" % "6.1.26" % "container",
    "commons-dbcp" % "commons-dbcp" % "1.4" % "container",
    "junit" % "junit" % "4.7" % "test",
    "ch.qos.logback" % "logback-classic" % "0.9.26",
    "com.h2database" % "h2" % "1.2.147")}