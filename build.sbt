name := "spark-fm"

organization := "io.edstud.spark"

version := "0.3.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0" % "provided"

libraryDependencies += "org.apache.spark"  % "spark-mllib_2.10" % "1.2.0"

libraryDependencies += "org.scalanlp" %% "breeze" % "0.10"

libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.10"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.8.0"