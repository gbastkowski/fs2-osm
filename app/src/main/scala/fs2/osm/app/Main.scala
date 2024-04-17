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
import java.time.*
import java.text.NumberFormat

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    parseArgs(args) match {
      case Some(config) =>
        for
          started  <- IO(LocalTime.now())
          summary  <- PostgresExporter[IO](config.db).run(Downloader[IO](config.uri).through(OsmEntityDecoder.pipe[IO]))
          finished <- IO(LocalTime.now())
          _        <- IO(println(prettySummary(summary, Duration.between(started, finished))))
        yield ExitCode.Success
      case _ => IO(ExitCode.Error)
    }

  private def prettySummary(summary: PostgresExporter.Summary, duration: Duration) =
    def prefixPad(i: Int) =
      val formattedNumber = NumberFormat.getInstance().format(i)
      " ".repeat(12 - formattedNumber.length) + formattedNumber
    val formattedDuration = (duration.toSeconds / 3600, duration.toSeconds / 60, duration.toSeconds % 60) match {
      case (0,     0,       seconds) => s"$seconds seconds"
      case (0,     minutes, seconds) => s"$minutes minutes, $seconds seconds"
      case (hours, minutes, seconds) => s"$hours hours, $minutes minutes, $seconds seconds"
    }
    val ni = prefixPad(summary.nodes.inserted)
    val nu = prefixPad(summary.nodes.updated)
    val nd = prefixPad(summary.nodes.deleted)
    val wi = prefixPad(summary.ways.inserted)
    val wu = prefixPad(summary.ways.updated)
    val wd = prefixPad(summary.ways.deleted)
    val ri = prefixPad(summary.relations.inserted)
    val ru = prefixPad(summary.relations.updated)
    val rd = prefixPad(summary.relations.deleted)
    s"""
      |               | nodes        | ways         | relations
      |  -------------+--------------+--------------+--------------
      |  inserted     | ${ni       } | ${wi       } | ${ri       }
      |  updated      | ${nu       } | ${wu       } | ${ru       }
      |  deleted      | ${nd       } | ${wd       } | ${rd       }
      |  -------------+--------------+--------------+--------------
      |  elapsed time: $formattedDuration
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
