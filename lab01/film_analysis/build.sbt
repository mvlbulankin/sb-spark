ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"

lazy val root = (project in file("."))
  .settings(
    name := "film_analysis"
  )
