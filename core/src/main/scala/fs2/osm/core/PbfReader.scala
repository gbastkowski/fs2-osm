package fs2.osm
package core

import cats.effect.*
import cats.syntax.all.*
import fs2.{Chunk, Pull, Stream}
import org.apache.logging.log4j.scala.Logging
import org.openstreetmap.osmosis.osmbinary.fileformat.*

private[core] object PbfReader extends Logging {
  def pipe[F[_]](bytes: Stream[F, Byte]): Stream[F, (BlobHeader, Blob)] =
    bytes
      .repeatPull { pull =>
        pull.unconsN(4, allowFewer = true).flatMap {
          case Some((size, tail)) => tail.pull.unconsN(parseBigEndian(size)).flatMap {
            case Some((headerChunk, tail)) =>
              logger.debug(s"headerChunk.size = ${headerChunk.size}")
              val header = BlobHeader.parseFrom(headerChunk.toArray)
              tail.pull.unconsN(header.datasize).flatMap {
                case Some(blob, tail)  => Pull.output1((header, Blob.parseFrom(blob.toArray))) as tail.some
                case _                 => Pull.pure(Option.empty)
              }
            case None =>
              logger.debug(s"nothing more to pull")
              Pull.pure(Option.empty)
          }
          case None => Pull.pure(Option.empty)
        }
      }
      .debug(toDebugString, logger.debug(_))

  private def toDebugString(h: BlobHeader, b: Blob) =
    h.`type` +
    b.raw
      .map(_ => " uncompressed ")
      .orElse(b.zlibData.map(_ => " compressed "))
      .getOrElse(" invalid ") +
    b.rawSize.getOrElse("unknown") + " bytes"

  private def parseBigEndian(c: Chunk[Byte]) = {
    val result = java.nio.ByteBuffer.wrap(c.toArray).getInt
    logger.debug(s"parseBigEndian = $result")
    result
  }
}
