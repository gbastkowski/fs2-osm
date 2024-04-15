import Dependencies._

ThisBuild / scalaVersion   := "3.3.1"
Global / excludeLintKeys   := Set(fork) // remove sbt warning

lazy val core = project
  .settings(commonSettings)
  .settings(
    libraryDependencies   ++= fs2                                   ++
                              logging                               ++
                              sttp                                  ,
    Compile / PB.targets   := Seq(scalapb.gen(grpc = false) -> (Compile / sourceManaged).value)
  )

lazy val postgres = project
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    libraryDependencies   ++= pureconfig                            ++
                              doobie                                ++
                              embeddedPostgres  .map  { _ % Test }  ++
                              fs2                                   ++
                              logging                               ,
    run / fork             := true
  )

lazy val it = project
  .dependsOn(core, postgres)
  .settings(commonSettings)
  .settings(
    libraryDependencies   ++= embeddedPostgres  .map  { _ % Test }  ,
    test / fork            := true
  )

lazy val root = project.in(file("."))
  .aggregate(core, it, postgres)

lazy val commonSettings = Seq(
  libraryDependencies     ++= weaver            .map  { _ % Test }  ,
  testFrameworks           += new TestFramework("weaver.framework.CatsEffect")
)
