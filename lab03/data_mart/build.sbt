ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3" % Provided,
  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.2" % Provided,
  "org.postgresql" % "postgresql" % "42.3.3" % Provided,
)

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )
