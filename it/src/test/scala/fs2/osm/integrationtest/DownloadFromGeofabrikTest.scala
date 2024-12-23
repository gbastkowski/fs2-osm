package fs2.osm
package integrationtest

import cats.effect.*
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import doobie.util.transactor.Transactor
import fs2.Stream
import fs2.osm.core.*
import java.sql.Connection
import javax.sql.DataSource
import org.testcontainers.utility.DockerImageName
import postgres.*
import sttp.client3.UriContext
import sttp.model.Uri
import telemetry.Telemetry
import weaver.*

object DownloadFromGeofabrikTest extends IOSuite {
  private val germany = uri"http://download.geofabrik.de/europe/germany-latest.osm.pbf"
  private val bremen  = uri"http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf"

  private val features = List(
    HighwayFeature,
    // WaterFeature,
    // BuildingFeature,
    // RailwayFeature,
    // ProtectedAreaFeature
  )

  override type Res = PostgresExporter[IO]
  override def sharedResource: Resource[IO, Res] =
    for xa <- if (isCi) embeddedPostgres else installedPostgres
    yield new PostgresExporter[IO](features, Telemetry.noop, xa)

  test("download Bremen data from web and export to Postgres") { exporter =>
    val bytes   = Downloader[IO](bremen)
    val stream  = bytes.through(OsmEntityDecoder.pipe)
    for
      cancel   <- Deferred[IO, Either[Throwable, Unit]]
      _        <- (IO.async_[Unit] { cb =>
                    sun.misc.Signal.handle(new sun.misc.Signal("INT"),  _ => cb(Right(())))
                    sun.misc.Signal.handle(new sun.misc.Signal("TERM"), _ => cb(Right(())))
                  } *> cancel.complete(Right(()))).start
      summary  <- exporter.runExport(stream.interruptWhen(cancel))
    yield expect.all(
      summary.get("nodes").inserted > 10000,
      summary.get("ways").inserted > 10000,
    )
  }

  test("check some nodes from offline Bremen data") {
    val bytes = Downloader[IO](uri"http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf")
    val entities = bytes through OsmEntityDecoder.pipe
    for
      sample               <- entities.find { entity =>
                                Seq(
                                  2991627733L,   // Becks Brewery Tour
                                  2619471455L,   // Loriot-Sofa
                                  4294473L,      // way Oldenburger Straße
                                  125799L,       // node on the way
                                  30210517L,     // bridge on Oldenburger Straße
                                  302943672L     // Metropol hotel/pub
                                ).contains(entity.osmId)
                              }.compile.toList
      oldenburgerStrasse    = sample.collect { case w: Way  if w.osmId ==     4294473L => w }
      // loriotSofa            = sample.collect { case n: Node if n.osmId ==  2619471455L => n }.head
      // metropol              = sample.collect { case n: Node if n.osmId ==   302943672L => n }.head
      found                 = sample.collect { case n: Node if n.osmId ==      125799L => n }.head
      // becks                 = sample.collect { case n: Node if n.osmId ==  2991627733L => n }.head
      // bridge                = sample.collect { case n: Node if n.osmId == 30210517L => n }.head
    yield expect.all(
      // sample.size == 2,
      // oldenburgerStrasse.size == 1,
      // oldenburgerStrasse.tags("highway") == "trunk",
      found.coordinate  == Coordinate(8.7868047, 53.0749415),
      // bridge.coordinate == Coordinate(8.7882187, 53.0770151)
    )
  }

  private lazy val isCi = sys.env.get("CI").nonEmpty

  private lazy val installedPostgres: Resource[IO, Transactor[IO]] =
    Resource.pure(postgres.Config("jdbc:postgresql:fs2-osm", sys.props("user.name"), "").transactor)

  private lazy val embeddedPostgres: Resource[IO, Transactor[IO]] =
    Resource
      .make {
        IO {
          EmbeddedPostgres
            .builder()
            .setImage(DockerImageName.parse("postgis/postgis"))
            .start()
            .getPostgresDatabase()
            .getConnection()
        }
      } { c => IO(c.close()) }
      .map  { conn => Transactor.fromConnection(conn, logHandler = Option.empty) }
}
