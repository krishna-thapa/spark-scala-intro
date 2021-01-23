name := "spark-scala-intro"

version := "0.1"

// Have to use Scala 2.12 for latest Spark dependencies
scalaVersion := "2.12.13"

val sparkVersion = "3.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)


