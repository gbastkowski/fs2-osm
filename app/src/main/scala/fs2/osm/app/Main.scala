package fs2.osm
package app

import cats.effect.*
import fs2.osm.core.*
import fs2.osm.postgres.{PostgresExporter, RelationBuilder, Summary, SummaryItem}
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import scopt.OParser
import sttp.client3.UriContext
import sttp.model.Uri
import java.time.*
import java.text.NumberFormat
import scala.collection.immutable

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    parseArgs(args) match {
      case Some(config) =>
        for
          started  <- IO(LocalTime.now())
          xa        = config.db.transactor[IO]
          summary  <- new PostgresExporter[IO](xa).run(Downloader[IO](config.uri).through(OsmEntityDecoder.pipe[IO]))
          summary  <- new RelationBuilder[IO](xa).run(summary)
          finished <- IO(LocalTime.now())
          _        <- IO(println(prettySummary(summary, Duration.between(started, finished))))
        yield ExitCode.Success
      case _ => IO(ExitCode.Error)
    }

  private def prettySummary(summary: Summary, duration: Duration) =
    val formattedDuration = (duration.toSeconds / 3600, duration.toSeconds / 60, duration.toSeconds % 60) match {
      case (0,     0,       seconds) => s"$seconds seconds"
      case (0,     minutes, seconds) => s"$minutes minutes, $seconds seconds"
      case (hours, minutes, seconds) => s"$hours hours, $minutes minutes, $seconds seconds"
    }

    val columnWidth =
      Set(
        summary.operations.keySet.map(_.length).max,
        NumberFormat.getInstance().format(summary.operations.values.map { _.inserted }.max).length,
        NumberFormat.getInstance().format(summary.operations.values.map { _.updated  }.max).length,
        NumberFormat.getInstance().format(summary.operations.values.map { _.deleted  }.max).length,
        "elapsed time:".length
      ).max

    def prefixPad(i: Int) = String.format("%" + columnWidth + "s", NumberFormat.getInstance().format(i))

    def row(columns: List[String], padding: Char, separator: Char) =
      columns
        .map { _.padTo(columnWidth, padding) }
        .map { padding + _ + padding }
        .mkString(separator.toString)

    List(
      List(
        row(List(" ", "inserted", "updated", "deleted"), ' ', '|'),
        row(List("-", "-", "-", "-"), '-', '+')
      ),
      summary.operations.toList.map {
        case (key, SummaryItem(inserted, updated, deleted)) =>
          row(List(key, prefixPad(inserted), prefixPad(updated), prefixPad(deleted)), ' ', '|')
      },
      List(
        row(List("-", "-", "-", "-"), '-', '+'),
        row(List("elapsed time:", s"$formattedDuration"), ' ', '|')
      )
    )
      .flatten
      .map { "  " + _ }
      .mkString("\n")

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
