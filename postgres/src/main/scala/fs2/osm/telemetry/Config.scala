package fs2.osm
package telemetry

import java.time.Duration
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*

object Config {
  val empty: Config = Config(Option.empty, false, false, Duration.ofSeconds(2))
}

case class Config(
  prometheusPort: Option[Int],
  loggingExportEnabled: Boolean,
  otlpGrpcExportEnabled: Boolean,
  interval: Duration
) derives ConfigReader
