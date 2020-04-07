name := "spark-nasa-topn"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.scalamock" %% "scalamock" % "4.0.0",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test",
  "com.github.scopt" %% "scopt" % "3.5.0"
)

resolvers += "jitpack" at "https://jitpack.io"