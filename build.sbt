import Dependencies._

ThisBuild / scalaVersion   := "3.3.1"
Global / excludeLintKeys   := Set(fork) // remove sbt warning

lazy val core = project
  .enablePlugins(GitVersioning)
  .settings(commonSettings)
  .settings(
    git.useGitDescribe     := true,
    libraryDependencies   ++= fs2                                   ++
                              logging                               ++
                              sttp                                  ,
    Compile / PB.targets   := Seq(scalapb.gen(grpc = false) -> (Compile / sourceManaged).value))

lazy val postgres = project
  .enablePlugins(GitVersioning)
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    git.useGitDescribe     := true,
    libraryDependencies   ++= pureconfig                            ++
                              doobie                                ++
                              embeddedPostgres  .map  { _ % Test }  ++
                              fs2                                   ++
                              logging                               )

lazy val it = project
  .dependsOn(core, postgres)
  .settings(commonSettings)
  .settings(
    libraryDependencies   ++= embeddedPostgres  .map  { _ % Test }  )

lazy val app = project
  .enablePlugins(BuildInfoPlugin, GitVersioning)
  .dependsOn(postgres)
  .settings(commonSettings)
  .settings(
    buildInfoKeys          := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage       := "fs2.osm.app",
    git.useGitDescribe     := true,
    libraryDependencies   ++= scopt)

lazy val root = project.in(file("."))
  .aggregate(app, core, it, postgres)

lazy val commonSettings = Seq(
  libraryDependencies     ++= weaver            .map  { _ % Test },
  testFrameworks           += new TestFramework("weaver.framework.CatsEffect"),
  run / fork               := true,
  test / fork              := true)
