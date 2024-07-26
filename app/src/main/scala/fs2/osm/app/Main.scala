package fs2.osm
package app

import cats.effect.*
import fs2.osm.core.*
import fs2.osm.postgres.PostgresExporter
import fs2.osm.telemetry.Telemetry
import java.time.*
import sttp.client3.UriContext

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    val resources = for
      config     <- Resource.eval(IO.fromOption(CliArgumentParser(args)) { new IllegalArgumentException("Invalid arguments") })
      telemetry  <- Telemetry[IO](BuildInfo.name, BuildInfo.version, config.uri.toString)
    yield (config, telemetry)
    resources.use { program }

  private val features = List(
    postgres.ImporterPropertiesFeature,
    postgres.OsmLineFeature,
    postgres.HighwayFeature,
    postgres.WaterFeature,
    postgres.BuildingFeature,
    postgres.RailwayFeature,
    postgres.ProtectedAreaFeature)

  private def program(config: Config, otel: Telemetry[IO]): IO[ExitCode] =
    for
      started    <- IO(LocalTime.now())
      xa          = config.db.transactor[IO]
      cancel     <- Deferred[IO, Either[Throwable, Unit]]
      _          <- (IO.async_[Unit] { cb =>
                      sun.misc.Signal.handle(new sun.misc.Signal("INT"),  _ => cb(Right(())))
                      sun.misc.Signal.handle(new sun.misc.Signal("TERM"), _ => cb(Right(())))
                    } *> cancel.complete(Right(()))).start
      exporter    = new PostgresExporter[IO](features, otel, xa)
      summary    <- exporter.run(Downloader[IO](config.uri).interruptWhen(cancel).through(OsmEntityDecoder.pipe[IO]))
      finished   <- IO(LocalTime.now())
      _          <- IO(println(PrettySummary(summary, Duration.between(started, finished))))
    yield ExitCode.Success
}
