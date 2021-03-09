name := "SparkApp"

version := "0.1"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"
val csvVersion = "1.5.0"

libraryDependencies in ThisBuild ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies += "com.databricks" %% "spark-csv" % csvVersion



