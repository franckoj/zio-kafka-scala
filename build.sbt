ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "ZIO-Kafka"
  )
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-kafka"   % "2.0.0-M2",
  "dev.zio" %% "zio-json"    % "0.3.0-RC3"
  )
