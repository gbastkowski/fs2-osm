package fs2.osm
package core

import cats.effect.*
import cats.syntax.all.*
import sttp.client3.UriContext
import sttp.model.Uri
import weaver.*

object DownloadFileSpec extends SimpleIOSuite {
  private val uri = uri"http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf"

  loggedTest("download and convert to entities") { logger =>
    for
      numberOfBlobs <- Downloader[IO](uri).compile.count
    yield expect(numberOfBlobs >= 244)
  }

  loggedTest("download to temp file") { logger =>
    for
      path <- Downloader.toFile[IO](uri)
      _    <- logger.debug(path.toString)
    yield expect(true)
  }
}
