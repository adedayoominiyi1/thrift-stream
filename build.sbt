
organization := "nl.grons"

name := "reactive-thrift"

version := "0.0.0"

description <<= (scalaVersion) { sv =>
  "metrics-scala for Scala " + sbt.cross.CrossVersionUtil.binaryScalaVersion(sv)
}

scalaVersion := "2.10.6"

crossScalaVersions := Seq("2.10.6", "2.11.8")

crossVersion := CrossVersion.binary

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

libraryDependencies <++= (scalaVersion) { sv =>
  Seq(
    "uk.co.real-logic" % "Agrona" % "0.4.10",
    "org.scodec" %% "scodec-bits" % "1.0.12",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
    // Override version that scalatest depends on:
    "org.scala-lang" % "scala-reflect" % sv % "test",
    "org.mockito" % "mockito-all" % "1.10.19" % "test"
  )
}

javacOptions ++= Seq("-Xmx512m", "-Xms128m", "-Xss10m", "-source", "1.6", "-target", "1.6")

javaOptions ++= Seq("-Xmx512m", "-Djava.awt.headless=true")

scalacOptions ++= Seq("-deprecation", "-unchecked")

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials(Path.userHome / ".sbt" / "sonatype.credentials")

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

// TODO: determine license
licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

pomExtra := (
  <url>https://github.com/erikvanoosten/todo</url>
  <scm>
    <url>git@github.com:erikvanoosten/todo.git</url>
    <connection>scm:git:git@github.com:erikvanoosten/todo.git</connection>
  </scm>
  <developers>
    <developer>
      <name>Erik van Oosten</name>
      <url>http://day-to-day-stuff.blogspot.com/</url>
    </developer>
  </developers>
)
