ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
)

lazy val root = (project in file("."))
  .settings(
    name := "lab02"
  )
