name := "spark-read-zip-format"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0" // "provided" // add this if you wanna compile and package for deployment into a Spark Cluster