import Dependencies._
import sbtassembly.MergeStrategy

name := "access-log-analyser"
scalaVersion := scalaVersionNumber
organization := artifactGroupName
version      := artifactVersionNumber

libraryDependencies ++= Seq(
  accessLogParser,
  sbsRuntime,
  scalaTest % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.rename
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in assembly := Some("com.github.apuex.accesslog.analyser.Main")
assemblyJarName in assembly := s"${name.value}.jar"

publishTo := sonatypePublishTo.value
