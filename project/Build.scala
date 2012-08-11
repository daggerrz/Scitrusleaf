import sbt._
import Keys._

object Scitrusleaf extends Build {

  val buildOrganization = "com.tapad"
  val buildVersion = "1.0.0-SNAPSHOT"
  val buildScalaVersion = "2.9.2"

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := buildOrganization,
    version := buildVersion,
    scalaVersion := buildScalaVersion,
    resolvers += "twttr.com" at "http://maven.twttr.com",
    resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
  )


  val dependencies = Seq(
    "ch.qos.logback" % "logback-classic" % "0.9.24" % "runtime",
    "org.slf4j" % "slf4j-api" % "1.6.1",
    "com.twitter" % "finagle-core" % "5.1.0",
    "com.twitter" % "finagle-stream" % "5.1.0",
    "com.twitter" % "util-core" % "5.2.1-SNAPSHOT",
    "io.netty" % "netty" % "3.5.2.Final",
    "com.typesafe.akka" % "akka-actor" % "2.0.2",
    "org.specs2" %% "specs2" % "1.11" % "test"
  )

 lazy val root = Project(
    "scitrusleaf", file("."),
    settings =
      buildSettings ++
      Seq(
        libraryDependencies ++= dependencies
      )
  )
}
