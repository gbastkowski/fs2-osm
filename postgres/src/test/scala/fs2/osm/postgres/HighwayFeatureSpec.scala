package fs2.osm
package postgres

import cats.*
import cats.effect.*
import cats.syntax.all.*
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import doobie.util.transactor.Transactor
import fs2.*
import fs2.osm.core.*
import fs2.osm.telemetry.*
import org.scalacheck.Gen
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{Counter, MeasurementValue}
import scala.collection.immutable
import weaver.*
import weaver.scalacheck.*

object HighwayFeatureSpec extends IOSuite with Checkers:
  override type Res = PostgresExporter[IO]
  override def sharedResource: Resource[IO, Res] =
    val acquire = IO {
      EmbeddedPostgres
        .builder()
        .setImage(DockerImageName.parse("postgis/postgis"))
        .start()
        .getPostgresDatabase()
        .getConnection()
    }
    for
      conn       <- Resource.make(acquire) { c => IO(c.close()) }
      transactor  = Transactor.fromConnection(conn, logHandler = Option.empty)
    yield new PostgresExporter[IO](features = List(HighwayFeature), Telemetry.noop, transactor)

  test("highways table available") { exporter =>
    for
      _      <- exporter.initSchema
      exists <- exporter.tableExists("highways")
    yield expect(exists)
  }
