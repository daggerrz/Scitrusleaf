import sbt._
import Keys._

object Scitrusleaf extends Build {

  val buildOrganization = "com.tapad"
  val buildVersion = "1.0.0-SNAPSHOT"
  val buildScalaVersion = "2.9.1"

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := buildOrganization,
    version := buildVersion,
    scalaVersion := buildScalaVersion,
    resolvers += "twttr.com" at "http://maven.twttr.com"
  )


  val dependencies = Seq(
    "ch.qos.logback" % "logback-classic" % "0.9.24" % "runtime",
    "com.twitter" %% "finagle-core" % "3.0.0",
    "com.twitter" %% "finagle-stream" % "3.0.0",
    "io.netty" % "netty" % "3.4.0.Alpha1",
    "org.specs2" %% "specs2" % "1.5" % "test"
  )

 lazy val root = Project(
    "root", file("."),
    settings =
      buildSettings ++
      Seq(
        libraryDependencies ++= dependencies
      )
  )
}
