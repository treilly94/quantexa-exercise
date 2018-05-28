name := "quantexa-exercise"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.scalatest/scalatest
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.3.0"
)