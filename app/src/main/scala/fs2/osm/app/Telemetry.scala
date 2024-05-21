package fs2.osm
package app

import cats.effect.Sync
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.exporter.logging.LoggingMetricExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.PeriodicMetricReader

object Telemetry {
  def apply[F[_]: Sync]: F[Telemetry[F]] = Sync[F].delay(new Telemetry[F]())
}

class Telemetry[F[_]: Sync]() {
  def addCounter(name: String, description: String) =
    Sync[F].delay(meter.counterBuilder(name).setDescription(description).setUnit("1").build())

  lazy val openTelemetry: OpenTelemetry =
    OpenTelemetrySdk.builder()
      .setMeterProvider(sdkMeterProvider)
      .build()

  private lazy val meter = openTelemetry.meterBuilder("fs2osm").build()

  private lazy val sdkMeterProvider =
    SdkMeterProvider.builder()
      .registerMetricReader(PeriodicMetricReader.builder(metricExporter).setInterval(interval).build())
      .build()

  private lazy val metricExporter = LoggingMetricExporter.create()
  private lazy val interval = java.time.Duration.ofSeconds(5)
}
