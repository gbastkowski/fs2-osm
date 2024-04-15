import Dependencies._

ThisBuild / scalaVersion := "3.3.1"

lazy val core = project
  .settings(
    libraryDependencies   ++= fs2                                   ++
                              logging                               ++
                              sttp                                  ++
                              weaver            .map  { _ % Test }  ,
    Compile / PB.targets   := Seq(scalapb.gen(grpc = false) -> (Compile / sourceManaged).value),
    testFrameworks         += new TestFramework("weaver.framework.CatsEffect")
  )

lazy val postgres = project
  .dependsOn(core)
  .settings(
    libraryDependencies   ++= pureconfig                            ++
                              doobie                                ++
                              fs2                                   ++
                              logging                               ++
                              weaver            .map  { _ % Test }  ,
    run / fork             := true,
    testFrameworks         += new TestFramework("weaver.framework.CatsEffect")
  )

lazy val it = project
  .dependsOn(core, postgres)
  .settings(
    libraryDependencies   ++= embeddedPostgres  .map  { _ % Test }  ++
                              weaver            .map  { _ % Test }  ,
    test / fork            := true,
    testFrameworks         += new TestFramework("weaver.framework.CatsEffect")
  )

lazy val root = project.in(file("."))
  .aggregate(core, it, postgres)
