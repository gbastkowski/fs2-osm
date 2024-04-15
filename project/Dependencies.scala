import sbt._

object Dependencies {
  object Versions {
    val doobie            = "1.0.0-RC4"
    val embeddedPostgres  = "2.0.7"
    val fs2               = "3.9.4"
    val http4s            = "0.23.26"
    val log4cats          = "2.6.0"
    val log4j             = "2.23.1"
    val log4jScala        = "13.1.0"
    val odin              = "0.13.0"
    val postgisJdbc       = "2.5.1"
    val pureconfig        = "0.17.6"
    val scalaLogging      = "3.9.4"
    val sttp              = "3.9.5"
    val weaver            = "0.8.4"
  }

  val pureconfig = Seq(
    "com.github.pureconfig"          %% "pureconfig-core"         % Versions.pureconfig,
    "com.github.pureconfig"          %% "pureconfig-cats"         % Versions.pureconfig,
    "com.github.pureconfig"          %% "pureconfig-cats-effect"  % Versions.pureconfig)

  val doobie = Seq(
    "org.tpolecat"                   %% "doobie-core"             % Versions.doobie,
    "org.tpolecat"                   %% "doobie-hikari"           % Versions.doobie,
    "org.tpolecat"                   %% "doobie-postgres"         % Versions.doobie,
    "org.tpolecat"                   %% "doobie-postgres-circe"   % Versions.doobie,
    "net.postgis"                     % "postgis-jdbc"            % Versions.postgisJdbc)

  val embeddedPostgres = Seq(
    "io.zonky.test"                   % "embedded-postgres"       % Versions.embeddedPostgres)

  val fs2 = Seq(
    "co.fs2"                         %% "fs2-core"                % Versions.fs2,
    "co.fs2"                         %% "fs2-io"                  % Versions.fs2)

  val http4s = Seq(
    "org.http4s"                     %% "http4s-ember-client"       % Versions.http4s,
    "org.http4s"                     %% "http4s-blaze-client"       % "0.23.16",
    "org.http4s"                     %% "http4s-dsl"                % Versions.http4s)

  val logging = Seq(
    // "com.github.valskalla"           %% "odin-core"         % Versions.odin,
    "org.apache.logging.log4j"        % "log4j-core"              % Versions.log4j,
    "org.apache.logging.log4j"       %% "log4j-api-scala"         % Versions.log4jScala)

  val sttp = Seq(
     "com.softwaremill.sttp.client3" %% "fs2"                     % Versions.sttp)

  val weaver = Seq(
    "com.disneystreaming"            %% "weaver-cats"             % Versions.weaver,
    "com.disneystreaming"            %% "weaver-scalacheck"       % Versions.weaver)
}
