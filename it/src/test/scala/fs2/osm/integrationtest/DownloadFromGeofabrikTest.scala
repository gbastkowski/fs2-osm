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
import weaver.*

object DownloadFromGeofabrikTest extends IOSuite {
  override type Res = PostgresExporter[IO]
  override def sharedResource: Resource[IO, Res] = {
    val acquire = IO {
      val embedded = EmbeddedPostgres.builder().setImage(DockerImageName.parse("postgis/postgis")).start()
      embedded.getPostgresDatabase().getConnection()
    }

    Resource
      .make(acquire) { c => IO(c.close()) }
      .map { conn => new PostgresExporter[IO](Transactor.fromConnection(conn, logHandler = Option.empty)) }

    // Resource
    //   .eval(IO(new PostgresExporter[IO](postgres.Config("jdbc:postgresql:fs2-osm", "gunnar.bastkowski", "").transactor)))
  }

  test("download Bremen data from web and export to Postgres") { exporter =>
    val bytes = Downloader[IO](uri"http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf")
    for
      summary  <- exporter run (bytes through OsmEntityDecoder.pipe)
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
}
