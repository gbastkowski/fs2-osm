package fs2.osm
package app

import java.time.Duration
import scopt.OParser
import sttp.model.Uri

object CliArgumentParser {
  def apply(args: List[String]): Option[Config] = OParser.parse(parser, args, empty)

  private lazy val builder = OParser.builder[Config]
  import builder.*

  private lazy val parser = OParser.sequence(
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
      .action       { (u, config) => config.copy(uri = Uri.unsafeParse(u)) })

  private lazy val empty =
    Config(
      uri = Uri(""),
      db  = postgres.Config("", "", ""),
      otel = telemetry.Config.empty
    )
}
