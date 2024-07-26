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

object EntityDecoderIntegrationTest extends SimpleIOSuite {
  private val bytes = readInputStream(
    fis           = IO(getClass.getResourceAsStream("/bremen-20240402.osm.pbf")),
    chunkSize     = 1024,
    closeAfterUse = true)

  test("has nodes and ways") {
    val entities = bytes through OsmEntityDecoder.pipe
    for
      nodes  <- entities.collect { case n: Node  => n }.compile.count
      ways   <- entities.collect { case w: Way   => w }.compile.count
    yield expect.all(
      nodes > 0,
      ways > 0)
  }
}
