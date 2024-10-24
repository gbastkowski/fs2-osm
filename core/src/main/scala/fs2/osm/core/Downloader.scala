package fs2.osm
package core

import cats.effect.*
import cats.syntax.all.*
import fs2.Stream
import fs2.io.file.{Files, Path}
import org.apache.logging.log4j.scala.Logging
import org.openstreetmap.osmosis.osmbinary.fileformat.{BlobHeader, Blob}
import scala.concurrent.duration.Duration
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.*
import sttp.client3.httpclient.fs2.HttpClientFs2Backend
import sttp.model.Uri

object Downloader extends Logging {
  def apply[F[_]: Async](uri: Uri): Stream[F, Byte] =
    uri.scheme.getOrElse("file") match {
      case "file" => Files[F].readAll(Path(Uri.AbsolutePath(uri.pathSegments.segments.toSeq).toString))
      case _      => downloadHttp(uri)
    }

  private def downloadHttp[F[_]: Async](uri: Uri) =
    for
      backend  <- Stream.resource(HttpClientFs2Backend.resource())
      response <- Stream.eval(createRequest(uri).send(backend))
      bytes    <- Stream.eval(handleError(response)).flatten
    yield bytes

  private def createRequest[F[_]](uri: Uri) =
    basicRequest
      .get(uri)
      .response(asStreamUnsafe(Fs2Streams[F]))
      .readTimeout(Duration.Inf)

  def toFile[F[_]: Async: Files](uri: Uri): F[Path] =
    HttpClientFs2Backend.resource().use { backend =>
      for
        response <- basicRequest
                      .get(uri)
                      .response(asStream(Fs2Streams[F]) { writeTempFile })
                      .readTimeout(Duration.Inf)
                      .send(backend)
        path     <- handleError(response)
      yield path
    }

  private def handleError[F[_]: Async, T](response: Response[Either[String, T]]): F[T] =
    response
      .body
      .leftMap { new RuntimeException(_) }
      .liftTo[F]

  private def writeTempFile[F[_]: Async: Files](bytes: Stream[F, Byte]) =
    for
      path <- Files[F].createTempFile
      _    <- bytes.through(Files[F].writeAll(path)).compile.drain
    yield path
}
