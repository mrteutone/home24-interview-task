name := "home24-interview-task"
version := "0.1"
scalaVersion := "2.12.10"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.rogach" %% "scallop" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.0.7" % Test
)
