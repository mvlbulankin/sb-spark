ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.12"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"

lazy val root = (project in file("."))
  .settings(
    name := "lab01"
  )
