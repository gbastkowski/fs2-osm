package fs2.osm
package app

import cats.effect.*
import fs2.osm.core.*
import fs2.osm.postgres.{PostgresExporter, Summary, SummaryItem}
import java.time.*
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import scopt.OParser
import sttp.client3.UriContext
import sttp.model.Uri

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] = parseArgs(args).fold(IO(ExitCode.Error)) { run }

  private val features = List(
    postgres.ImporterPropertiesFeature,
    postgres.OsmLineFeature,
    postgres.HighwayFeature,
    postgres.WaterFeature,
    postgres.BuildingFeature,
    postgres.RailwayFeature,
    postgres.ProtectedAreaFeature
  )

  private def parseArgs(args: List[String]): Option[Config] =
    val builder = OParser.builder[Config]
    import builder.*

    val parser = OParser.sequence(
      programName("osm2postgis"),
      head("osm2postgis", "0.1"),
      opt[String]('d', "jdbc-url")
        .required()
        .valueName("<jdbc-url>")
        .action { (d, config) => config.copy( db = config.db.copy(jdbcUrl = d)) }
        .text("JDBC connection specification"),
      opt[String]('u', "username")
        .required()
        .valueName("<username>")
        .action       { (u, config) => config.copy(db = config.db.copy(username = u)) }
        .withFallback { ()          => sys.env("USER") }
        .text("username for database connection"),
      opt[String]('p', "password")
        .valueName("<password>")
        .action       { (p, config) => config.copy(db = config.db.copy(password = p)) }
        .withFallback { ()          => "" }
        .text("password for database connection"),
      arg[String]("<URL>")
        .required()
        .action       { (u, config) => config.copy(uri = Uri.unsafeParse(u)) }
    )

    val empty = Config(
      uri = Uri(""),
      db  = postgres.Config("", "", "")
    )

    OParser.parse(parser, args, empty)

  private def run(config: Config): IO[ExitCode] =
    for
      telemetry  <- Telemetry.apply[IO]
      started    <- IO(LocalTime.now())
      nodesCount <- telemetry.addCounter("nodes_count", "Total number of nodes")
      _           = nodesCount.add(10)
      xa          = config.db.transactor[IO]
      cancel     <- Deferred[IO, Either[Throwable, Unit]]
      _          <- (IO.async_[Unit] { cb =>
                      sun.misc.Signal.handle(new sun.misc.Signal("INT"),  _ => cb(Right(())))
                      sun.misc.Signal.handle(new sun.misc.Signal("TERM"), _ => cb(Right(())))
                    } *> cancel.complete(Right(()))).start
      summary    <- new PostgresExporter[IO](features, xa)
                      .run(Downloader[IO](config.uri)
                      .interruptWhen(cancel)
                      .through(OsmEntityDecoder.pipe[IO]))
      finished   <- IO(LocalTime.now())
      _          <- IO(println(PrettySummary(summary, Duration.between(started, finished))))
    yield ExitCode.Success
