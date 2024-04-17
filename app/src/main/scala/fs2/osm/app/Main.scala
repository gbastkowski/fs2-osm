package fs2.osm
package app

import cats.effect.*
import fs2.osm.core.*
import fs2.osm.postgres.PostgresExporter
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import scopt.OParser
import sttp.client3.UriContext
import sttp.model.Uri

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    parseArgs(args) match {
      case Some(config) =>
        PostgresExporter[IO](config.db)
          .run(Downloader[IO](config.uri).through(OsmEntityDecoder.pipe[IO]))
          .map(s => println(prettySummary(s)))
          .as(ExitCode.Success)

      case _ => IO(ExitCode.Error)
    }

  private def prettySummary(summary: PostgresExporter.Summary) =
    val ni = summary.nodes.inserted.toString.padTo(12, ' ')
    val nu = summary.nodes.updated.toString.padTo(12, ' ')
    val nd = summary.nodes.deleted.toString.padTo(12, ' ')
    val wi = summary.ways.inserted.toString.padTo(12, ' ')
    val wu = summary.ways.updated.toString.padTo(12, ' ')
    val wd = summary.ways.deleted.toString.padTo(12, ' ')
    val ri = summary.relations.inserted.toString.padTo(12, ' ')
    val ru = summary.relations.updated.toString.padTo(12, ' ')
    val rd = summary.relations.deleted.toString.padTo(12, ' ')
    s"""
      |              | nodes      | ways       | relations
      |  ------------+------------+------------+------------
      |   inserted   | $ni        | $wi        | $ri
      |   updated    | $nu        | $wu        | $ru
      |   deleted    | $nd        | $wd        | $rd
      |  ------------+------------+------------+------------
      |   elapsed time: 3.000.000 seconds
    """.stripMargin

  private def parseArgs(args: List[String]): Option[Config] = {
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
  }
}
