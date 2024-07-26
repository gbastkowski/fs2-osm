addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo"       % "0.12.0")
addSbtPlugin("com.github.sbt"     % "sbt-git"             % "2.0.1")
addSbtPlugin("com.github.sbt"     % "sbt-github-actions"  % "0.24.0")
addSbtPlugin("com.lightbend.sbt"  % "sbt-javaagent"       % "0.1.6")
addSbtPlugin("com.thesamet"       % "sbt-protoc"          % "1.0.7")
addSbtPlugin("com.47deg"          % "sbt-microsites"      % "1.4.3")
// addSbtPlugin("com.github.sbt"     % "sbt-unidoc"          % "0.5.0")

libraryDependencies                  += "com.thesamet.scalapb"   %% "compilerplugin"  % "0.11.15"
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml"       % VersionScheme.Always
