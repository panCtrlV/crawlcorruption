name := "crawlcorruption"

version := "1.0"

scalaVersion := "2.11.7"

//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
//  "Local couchdb-scala repo" at (baseDirectory.value / "lib/couchdb-scala").toURI.toString
)


libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.8.3",
  "commons-codec" % "commons-codec" % "1.9",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.3",
  "com.couchbase.client" %% "spark-connector" % "1.0.0",
//  "com.databricks" % "spark-csv_2.10" % "1.2.0",
//  "org.scalanlp" %% "breeze" % "0.11.2",
//  "org.scalanlp" %% "breeze-natives" % "0.11.2",
//  "org.scalanlp" %% "breeze-viz" % "0.11.2",
//  "org.scala-saddle" %% "saddle-core" % "1.3.4",
  "com.ibm" % "couchdb-scala_2.11" % "0.6.0"
)

// Spark-related libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-yarn" % "1.5.2"
//  "org.apache.spark" %% "spark-core" % "1.5.2",
//  "org.apache.spark" %% "spark-streaming" % "1.5.2",
//  "org.apache.spark" %% "spark-mllib" % "1.5.2",
//  "org.apache.spark" %% "spark-sql" % "1.5.2"
)

libraryDependencies ++= Seq(
  "org.apache.opennlp" % "opennlp-tools" % "1.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2",
//  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" classifier "models",
  "edu.stanford.nlp" % "stanford-parser" % "3.5.2"
)

libraryDependencies ++= Seq(
  "org.springframework" % "spring-context" % "4.2.2.RELEASE",
  "org.springframework.boot" % "spring-boot-starter-parent" % "1.2.7.RELEASE",
  "org.springframework.boot" % "spring-boot-starter-web" % "1.2.7.RELEASE"
)

//libraryDependencies += "org.apache.tika" % "tika" % "1.11"  // downloaeded in .ivy2 but failed to add
//libraryDependencies += "org.apache.tika" % "tika-core" % "1.10"  // but tika-core is added successfully