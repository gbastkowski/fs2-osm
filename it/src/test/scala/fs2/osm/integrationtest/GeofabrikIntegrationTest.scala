package fs2.osm
package integrationtest

import cats.effect.*
import fs2.*
import fs2.io.readInputStream
import fs2.osm.core.*
import postgres.*
import sttp.client3.UriContext
import sttp.model.Uri
import weaver.*

object GeofabrikIntegrationTest extends SimpleIOSuite {
  private val online  = Downloader[IO](uri"http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf")
  private val offline = readInputStream(
                          fis           = IO(getClass.getResourceAsStream("/bremen-20240402.osm.pbf")),
                          chunkSize     = 1024,
                          closeAfterUse = true
                        )

  test("download Bremen data from web") {
    for
      exporter <- PostgresExporter[IO]
      count    <- exporter run (online through OsmEntityDecoder.pipe)
    yield expect(true)
  }

  test("has nodes and ways") {
    val entities = offline through OsmEntityDecoder.pipe
    for
      nodes  <- entities.collect { case n: Node  => n }.compile.count
      ways   <- entities.collect { case w: Way   => w }.compile.count
    yield expect.all(
      nodes > 0,
      ways > 0,
    )
  }

  test("check some nodes from offline Bremen data") {
    val entities = online through OsmEntityDecoder.pipe
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
      becks                 = sample.collect { case n: Node if n.osmId ==  2991627733L => n }.head
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
