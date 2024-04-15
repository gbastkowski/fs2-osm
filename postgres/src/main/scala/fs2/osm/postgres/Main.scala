package fs2.osm

import cats.effect.*
import fs2.osm.core.*
import fs2.osm.postgres.PostgresExporter
import pureconfig.*
import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import sttp.client3.UriContext
import sttp.model.Uri

object Main extends IOApp {
  case class Config(uri: Uri, db: PostgresExporter.Config) derives ConfigReader

  def run(args: List[String]): IO[ExitCode] = {
    for {
      config   <- parseArgs(args)
      entities  = Downloader[IO](config.uri).through(OsmEntityDecoder.pipe[IO])
      exporter <- PostgresExporter[IO]
      summary  <- exporter.run(entities)
    } yield ExitCode.Success
  }

  private def parseArgs(args: List[String]): IO[Config] =
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
    } yield
        Config(
          uri = uri"http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf",
          db = config.db
        )

  private given ConfigReader[Uri] = ConfigReader[String].map { Uri.unsafeParse }
}
