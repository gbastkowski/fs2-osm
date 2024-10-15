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
import org.typelevel.otel4s.metrics.{Counter, MeasurementValue, Meter}
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.oteljava.context.LocalContextProvider
import scala.collection.immutable

object Telemetry:
  def apply[F[_]: Async: LocalContextProvider](name: String, version: String, source: String): Resource[F, Telemetry[F]] =
    for   otel <- OtelJava.resource(openTelemetry(name, version))
    yield new Otel4sTelemetry(otel, attributes(source))

  def noop[F[_]: Async]: Telemetry[F] = new Telemetry[F] {
    val attributes: List[Attribute[?]] = Nil
    def counter[T: MeasurementValue](scope: String, instrument: String): F[SimpleCounter[F, T]] = Sync[F].pure {
      new SimpleCounter[F, T] {
        def add(value: T, attributes: immutable.Iterable[Attribute[?]]): F[Unit] = Sync[F].unit
        def add(value: T, attributes: Attribute[?]*): F[Unit] = Sync[F].unit
      }
    }
  }

  private def attributes(source: String) = List(Attribute(string("source"), source))

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
      .build())

  private lazy val interval = java.time.Duration.ofSeconds(2)

trait Telemetry[F[_]]:
  val attributes: List[Attribute[?]]
  def counter[T: MeasurementValue](scope: String, instrument: String): F[SimpleCounter[F, T]]

class Otel4sTelemetry[F[_]: Async: LocalContextProvider](otel: OtelJava[F], val attributes: List[Attribute[?]])
    extends Telemetry[F]:

  def meter(name: String): F[Meter[F]] = otel.meterProvider.get(name)

  def counter[T: MeasurementValue](scope: String, instrument: String): F[SimpleCounter[F, T]] =
    for
      meter    <- otel.meterProvider.get(scope)
      counter  <- meter.counter[T](instrument).create
    yield CounterFacade(counter)

trait SimpleCounter[F[_], A] {
  def add(value: A, attributes: immutable.Iterable[Attribute[?]]): F[Unit]
  def add(value: A, attributes: Attribute[?]*): F[Unit]
}

class CounterFacade[F[_], A](delegate: Counter[F, A]) extends SimpleCounter[F, A] {
  def add(value: A, attributes: immutable.Iterable[Attribute[?]]): F[Unit] = delegate.add(value, attributes)
  def add(value: A, attributes: Attribute[?]*): F[Unit] = delegate.add(value, attributes)
}
