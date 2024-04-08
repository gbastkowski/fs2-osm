package fs2.osm

import cats.effect.*
import cats.syntax.all.*
import fs2.*
import fs2.io.file.{Files, Path}
import org.openstreetmap.osmosis.osmbinary.fileformat.{BlobHeader, Blob}
import scala.concurrent.duration.Duration
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.*
import sttp.client3.httpclient.fs2.HttpClientFs2Backend
import sttp.model.Uri


object Downloader {
  def apply[F[_]: Async](uri: Uri): Stream[F, Byte] =
    // for {
    //     path   <- Stream.eval(toFile(uri))
    //     stream <- PbfReader.stream(Files[F].readAll(path))
    // } yield stream
    for {
      backend  <- Stream.resource(HttpClientFs2Backend.resource())
      response <- Stream.eval(createRequest(uri).send(backend))
      nested   <- handleError(response)
      bytes    <- nested
    } yield bytes

  private def createRequest[F[_]](uri: Uri) =
    basicRequest
      .get(uri)
      .response(asStreamUnsafe(Fs2Streams[F]))
      .readTimeout(Duration.Inf)

  private def handleError[F[_]: Async, T](response: Response[Either[String, T]]) =
    Stream.fromEither(response.body.leftMap { new RuntimeException(_) })

  def toFile[F[_]: Async: Files](uri: Uri) = {
    val effect = HttpClientFs2Backend.resource().use { backend =>
      basicRequest
        .get(uri)
        .response(asStream(Fs2Streams[F]) { writeTempFile })
        .readTimeout(Duration.Inf)
        .send(backend)
    }.flatMap { response => Async[F].fromEither(response.body.leftMap { message => new RuntimeException(message) })  }
    effect
  }

  private def writeTempFile[F[_]: Async: Files](bytes: Stream[F, Byte]) =
    for {
      path <- Files[F].createTempFile
      _    <- bytes.through(Files[F].writeAll(path)).compile.drain
    } yield path
}
