name := "SparkFM"

organization := "io.edstud.spark"

version := "0.1.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0" % "provided"

libraryDependencies += "org.apache.spark"  % "spark-mllib_2.10" % "1.2.0"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2"

libraryDependencies += "org.scalanlp" %% "breeze" % "0.10"

libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.10"
