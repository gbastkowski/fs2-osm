package fs2.osm
package telemetry

import cats.Applicative
import cats.Parallel
import cats.effect.std.Console
import cats.effect.{Async, Resource, Sync}
import cats.syntax.all.*
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.exporter.logging.LoggingMetricExporter
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.`export`.PeriodicMetricReader
import io.opentelemetry.sdk.resources.{Resource => JOtelResource}
import io.opentelemetry.sdk.{OpenTelemetrySdk => JOpenTelemetrySdk}
import io.opentelemetry.semconv.ResourceAttributes.*
import java.time.Duration
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.AttributeKey.string
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.oteljava.context.LocalContextProvider
import org.typelevel.otel4s.metrics.MeasurementValue
import org.typelevel.otel4s.metrics.Meter

object Telemetry:
  def apply[F[_]: Async: LocalContextProvider](name: String, version: String, source: String): Resource[F, Telemetry[F]] =
    for otel <- OtelJava.resource(openTelemetry(name, version))
    yield new Telemetry(otel, attributes(source))

  private def attributes(source: String): List[Attribute[String]] = List(Attribute(string("source"), source))

  private def openTelemetry[F[_]: Async](name: String, version: String) = Async[F].delay(
    JOpenTelemetrySdk.builder()
      .setMeterProvider(
        SdkMeterProvider.builder()
          .setResource(
            JOtelResource.getDefault().toBuilder()
              .put(SERVICE_NAME, name)
              .put(SERVICE_VERSION, version)
              .build())
          .registerMetricReader(PrometheusHttpServer.builder().setPort(8889).build())
          .registerMetricReader(PeriodicMetricReader.builder(LoggingMetricExporter.create()).setInterval(interval).build())
          // .registerMetricReader(PeriodicMetricReader.builder(OtlpGrpcMetricExporter.getDefault()).setInterval(interval).build())
          .build())
      .build()
    )

  private lazy val interval = java.time.Duration.ofSeconds(2)

class Telemetry[F[_]: Async: LocalContextProvider](otel: OtelJava[F], val attributes: List[Attribute[_]]):

  def meter(name: String): F[Meter[F]] = otel.meterProvider.get(name)

  def counter[T: MeasurementValue](scope: String, instrument: String) =
    for
      meter    <- otel.meterProvider.get(scope)
      counter  <- meter.counter[T](instrument).create
    yield counter

  // private def registerRuntimeMetrics[F[_]: Sync](openTelemetry: JOpenTelemetry): Resource[F, Unit] =
  //   val acquire = Sync[F].delay {
  //     List
  //       .newBuilder[AutoCloseable]
  //       .addAll(MemoryPools.registerObservers(openTelemetry).asScala)
  //       .addAll(Classes.registerObservers(openTelemetry).asScala)
  //       .addAll(Cpu.registerObservers(openTelemetry).asScala)
  //       .addAll(Threads.registerObservers(openTelemetry).asScala)
  //       .addAll(GarbageCollector.registerObservers(openTelemetry).asScala)
  //       .result()
  //   }

  //     Resource.make(acquire)(r => Sync[F].delay(r.foreach(_.close()))).void


// class Telemetry[F[_]: Sync]() {
//   def addCounter(name: String, description: String): F[Telemetry.Counter[F]] = Sync[F].delay {
//     val longCounter = importMeter.counterBuilder(name).setDescription(description).setUnit("1").build()
//     Telemetry.Counter(longCounter)
//   }

//   private lazy val openTelemetry: OpenTelemetry =
//     OpenTelemetrySdk.builder()
//       .setMeterProvider(sdkMeterProvider)
//       .build()

//   private lazy val importMeter = openTelemetry.meterBuilder("import").build()
