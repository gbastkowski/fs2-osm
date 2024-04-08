import Dependencies._

ThisBuild / scalaVersion := "3.3.1"

lazy val core = project
  .settings(
    libraryDependencies     ++= doobie                      ++
                                fs2                         ++
                                logging                     ++
                                sttp                        ++
                                weaver  .map  { _ % Test }  ,

    Compile / PB.targets     := Seq(
                                  scalapb.gen(grpc = false) -> (Compile / sourceManaged).value
                                ),

    testFrameworks           += new TestFramework("weaver.framework.CatsEffect")
  )
