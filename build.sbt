name := "FraudDetection"

version := "1.0"

libraryDependencies ++= Seq(
  "io.prediction" %% "core" % "0.9.3" % "provided",
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"
)