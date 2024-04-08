package fs2.osm

import cats.ApplicativeError
import cats.effect.*
import cats.syntax.all.*
import fs2.Stream
import scala.util.Either
import org.apache.logging.log4j.scala.Logging

object OsmEntityDecoder extends Logging {
  def pipe[F[_]: Sync](bytes: Stream[F, Byte]): Stream[F, OsmEntity] =
    for {
      (header, blob) <- PbfReader.stream(bytes)
      primitiveBlock <- Stream.fromEither(Either.fromTry(blob.toPrimitiveBlock))
      stringTable     = primitiveBlock.stringtable
      primitiveGroup <- Stream.emits(primitiveBlock.primitivegroup)
      entity         <- EntityStream(primitiveGroup, stringTable).debug(_.toString, logger.debug(_))
    } yield entity
}
