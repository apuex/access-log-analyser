import sbt._

object Dependencies {
  lazy val scalaVersionNumber    = "2.12.6"
  lazy val artifactVersionNumber = "1.0.0"
  lazy val artifactGroupName     = "com.github.apuex.accesslog"

  lazy val accessLogParser = "com.github.apuex.accesslog"%   "access-log-parser"                   % "1.0-SNAPSHOT"
  lazy val sbsRuntime      = "com.github.apuex.springbootsolution"%%   "runtime"                   % "1.0.6"
  lazy val jodaTime        = "joda-time"                 %   "joda-time"                           % "2.9.9"
  lazy val logback         = "ch.qos.logback"            %   "logback-classic"                     % "1.2.3"
  lazy val slf4jApi        = "org.slf4j"                 %  "slf4j-api"                            % "1.7.25"
  lazy val slf4jSimple     = "org.slf4j"                 %  "slf4j-simple"                         % "1.7.25"
  lazy val scalaTest       = "org.scalatest"             %% "scalatest"                            % "3.0.4"
  lazy val scalacheck      = "org.scalacheck"            %%  "scalacheck"                          % "1.13.4"

  lazy val localRepo = Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
}
