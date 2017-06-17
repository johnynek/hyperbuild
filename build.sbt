lazy val noPublish = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false)

lazy val hyperbuildSettings = Seq(
  organization := "org.typelevel",
  scalaVersion := "2.11.11",
  libraryDependencies ++= Seq(
    "ch.epfl.scala" %% "spores" % "0.4.3",
    "org.scalatest" %% "scalatest" % "3.0.1" % Test,
    "org.scalacheck" %% "scalacheck" % "1.13.5" % Test),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Ywarn-unused-import",
    "-Xfuture"),
  // HACK: without these lines, the console is basically unusable,
  // since all imports are reported as being unused (and then become
  // fatal errors).
  scalacOptions in (Compile, console) ~= {_.filterNot("-Xlint" == _)},
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
  addCompilerPlugin("ch.epfl.scala" %% "spores-serialization" % "0.4.3"))

lazy val commonJvmSettings = Seq(
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"))

lazy val hyperbuild = project
  .in(file("."))
  .settings(name := "root")
  .settings(hyperbuildSettings: _*)
  .settings(noPublish: _*)
  .aggregate(core)
  .dependsOn(core)

lazy val core = project
  .in(file("core"))
  .settings(name := "hyperbuild-core")
  .settings(moduleName := "hyperbuild-core")
  .settings(hyperbuildSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.twitter" %% "bijection-core" % "0.9.5",
    "org.typelevel" %% "cats-core" % "0.9.0",
    "org.typelevel" %% "cats-effect" % "0.3"
  ))
  .settings(commonJvmSettings:_*)

