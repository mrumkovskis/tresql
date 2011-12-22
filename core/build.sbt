name := "tresql"

organization := "org"

scalaVersion := "2.9.1"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "1.6.1" % "test", 
                            "org.hsqldb" % "hsqldb-j5" % "2.2.4" % "test")

publishTo := Some("Uniso Maven Repository" at "http://159.148.47.6:8087/artifactory/libs-release-local/")

credentials += Credentials(Path.userHome / ".ivy2" / "uniso-repo.credentials")