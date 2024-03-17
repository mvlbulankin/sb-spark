ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib"          % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided
)

lazy val root = (project in file("."))
  .settings(
    name := "mlproject"
  )
