package fs2.osm
package core

import cats.ApplicativeError
import cats.effect.*
import cats.syntax.all.*
import fs2.Stream
import org.apache.logging.log4j.scala.Logging
import scala.util.Either

object OsmEntityDecoder extends Logging {
  def pipe[F[_]: Sync](bytes: Stream[F, Byte]): Stream[F, OsmEntity] =
    for {
      (header, blob) <- bytes through PbfReader.pipe
      primitiveBlock <- Stream.fromEither(Either.fromTry(blob.toPrimitiveBlock))
      stringTable     = primitiveBlock.stringtable
      primitiveGroup <- Stream.emits(primitiveBlock.primitivegroup)
      entity         <- EntityStream(primitiveGroup, stringTable).debug(_.toString, logger.debug(_))
    } yield entity
}
