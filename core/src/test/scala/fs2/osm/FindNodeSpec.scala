package fs2.osm

import cats.effect.*
import cats.syntax.all.*
import fs2.*
import fs2.io.readInputStream
import scala.util.Either
import sttp.client3.UriContext
import sttp.model.Uri
import weaver.*

object FindNodeSpec extends SimpleIOSuite {
  private val online  = Downloader[IO](uri"http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf")
  private val offline = readInputStream(IO(getClass.getResourceAsStream("/bremen-20240402.osm.pbf")), 1024, closeAfterUse = true)

  test("download Bremen data from web") {
    val stream = for
      (header, blob) <- PbfReader.stream(offline)
      primitiveBlock <- Stream.fromEither(Either.fromTry(blob.toPrimitiveBlock))
      stringTable     = primitiveBlock.stringtable
      primitiveGroup <- Stream.emits(primitiveBlock.primitivegroup).filter { _.dense.nonEmpty }
      dense           = primitiveGroup.dense.get
      // _ = println(dense.id.head)
      // dense          <- DenseNodeStream(stringTable).nodeIds(primitiveGroup.dense.get)
    yield dense

    def delta(xs: Seq[Long]) = xs.tail.scan(xs.head) { _ + _ }

    for
      found <- stream.find(x => x.id.contains(8914620617L)).compile.toList
      // _      = delta(found.flatMap(_.id)).foreach(x => println("====== " + x))
    yield expect(true)
  }
}
