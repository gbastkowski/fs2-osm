addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo"   % "0.12.0")
addSbtPlugin("com.github.sbt"     % "sbt-git"         % "2.0.1")
addSbtPlugin("com.lightbend.sbt"  % "sbt-javaagent"   % "0.1.6")
addSbtPlugin("com.thesamet"       % "sbt-protoc"      % "1.0.7")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.15"
