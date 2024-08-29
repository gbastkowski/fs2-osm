import sbt._

object Versions {
  val doobie              = "1.0.0-RC5"
  val embeddedPostgres    = "1.1.0"
  val fs2                 = "3.11.0"
  val http4s              = "0.23.26"
  val log4cats            = "2.6.0"
  val log4j               = "2.23.1"
  val log4jScala          = "13.1.0"
  val odin                = "0.13.0"
  val opentelemetry       = "1.41.0"
  val opentelemetryAlpha  = "1.41.0-alpha"
  val otel4s              = "0.8.1"
  val postgisJdbc         = "2.5.1"
  val pureconfig          = "0.17.7"
  val scalaLogging        = "3.9.4"
  val scala               = "3.4.2"
  val scopt               = "4.1.0"
  val sttp                = "3.9.8"
  val weaver              = "0.8.4"
}

object Dependencies {
  val doobie = Seq(
    "org.tpolecat"                     %% "doobie-core"                               % Versions.doobie,
    "org.tpolecat"                     %% "doobie-hikari"                             % Versions.doobie,
    "org.tpolecat"                     %% "doobie-postgres"                           % Versions.doobie,
    "org.tpolecat"                     %% "doobie-postgres-circe"                     % Versions.doobie,
    "net.postgis"                       % "postgis-jdbc"                              % Versions.postgisJdbc)

  val embeddedPostgres = Seq(
    "com.opentable.components"          % "otj-pg-embedded"                           % Versions.embeddedPostgres)

  val fs2 = Seq(
    "co.fs2"                           %% "fs2-core"                                  % Versions.fs2,
    "co.fs2"                           %% "fs2-io"                                    % Versions.fs2)

  val http4s = Seq(
    "org.http4s"                       %% "http4s-ember-client"                       % Versions.http4s,
    "org.http4s"                       %% "http4s-blaze-client"                       % "0.23.16",
    "org.http4s"                     %% "http4s-dsl"                                % Versions.http4s)

  val logging = Seq(
    // "com.github.valskalla"             %% "odin-core"         % Versions.odin,
    "org.apache.logging.log4j"          % "log4j-core"                                % Versions.log4j,
    "org.apache.logging.log4j"          % "log4j-slf4j-impl"                          % Versions.log4j,
    "org.apache.logging.log4j"         %% "log4j-api-scala"                           % Versions.log4jScala)

  val otelCore = Seq(
    "org.typelevel"                    %% "otel4s-core"                               % Versions.otel4s)

  val otelJava = Seq(
    "io.opentelemetry"                  % "opentelemetry-exporter-logging"            % Versions.opentelemetry,
    "io.opentelemetry"                  % "opentelemetry-exporter-otlp"               % Versions.opentelemetry,
    "io.opentelemetry"                  % "opentelemetry-exporter-prometheus"         % Versions.opentelemetryAlpha,
    "io.opentelemetry.javaagent"        % "opentelemetry-javaagent"                   % "2.7.0",
    "io.opentelemetry"                  % "opentelemetry-sdk-extension-autoconfigure" % Versions.opentelemetry,
    "io.opentelemetry.instrumentation"  % "opentelemetry-runtime-telemetry-java17"    % "2.7.0-alpha",
    "org.typelevel"                    %% "otel4s-oteljava"                           % Versions.otel4s)

  val pureconfig = Seq(
    "com.github.pureconfig"            %% "pureconfig-core"                           % Versions.pureconfig,
    "com.github.pureconfig"            %% "pureconfig-cats"                           % Versions.pureconfig,
    "com.github.pureconfig"            %% "pureconfig-cats-effect"                    % Versions.pureconfig)

  val scopt = Seq(
    "com.github.scopt"                 %% "scopt"                                     % Versions.scopt)

  val sttp = Seq(
     "com.softwaremill.sttp.client3"   %% "fs2"                                       % Versions.sttp)

  val weaver = Seq(
    "com.disneystreaming"              %% "weaver-cats"                               % Versions.weaver,
    "com.disneystreaming"              %% "weaver-scalacheck"                         % Versions.weaver)
}
