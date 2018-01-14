name := "aisdecode"

version := "0.1"

libraryDependencies ++= Seq(
"org.scodec" %% "scodec-core" % "1.10.3",
"org.scodec" %% "scodec-bits" % "1.1.5"
)

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"

