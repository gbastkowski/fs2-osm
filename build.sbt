import Dependencies._

ThisBuild / scalaVersion             := Versions.scala
Global    / excludeLintKeys          := Set(fork) // remove sbt warning
ThisBuild / Test / parallelExecution := false

lazy val core = project
  .enablePlugins(GitVersioning)
  .settings(commonSettings)
  .settings(
    git.useGitDescribe     := true,
    libraryDependencies   ++= fs2                                     ++
                              logging                                 ++
                              sttp                                    ,
    Compile / PB.targets   := Seq(scalapb.gen(grpc = false) -> (Compile / sourceManaged).value))

lazy val postgres = project
  .enablePlugins(GitVersioning)
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    git.useGitDescribe     := true,
    libraryDependencies   ++= pureconfig                              ++
                              doobie                                  ++
                              embeddedPostgres  .map  { _ % Test }    ++
                              fs2                                     ++
                              logging                                 ++
                              otelCore                                ++
                              otelJava                                )

lazy val it = project
  .dependsOn(core, postgres)
  .settings(commonSettings)
  .settings(
    libraryDependencies   ++= embeddedPostgres  .map  { _ % Test }    ++
                              otelJava          .map  { _ % Test }    )

lazy val app = project
  .enablePlugins(BuildInfoPlugin, GitVersioning)
  .dependsOn(postgres)
  .settings(commonSettings)
  .settings(
    buildInfoKeys          := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage       := "fs2.osm.app",
    git.useGitDescribe     := true,
    // javaAgents             += "io.opentelemetry.javaagent"  % "opentelemetry-javaagent" % Versions.opentelemetry,
    javaOptions            += "-Dotel.javaagent.debug=true",
    libraryDependencies   ++= otelJava                                ++
                              scopt                                   ,
    name                   := "fs2-osm"                               )

lazy val root = project
  .in(file("."))
  .aggregate(app, core, it, postgres)

lazy val commonSettings = Seq(
  libraryDependencies     ++= weaver            .map  { _ % Test },
  testFrameworks           += new TestFramework("weaver.framework.CatsEffect"),
  run  / fork              := true,
  test / fork              := true)
