package fs2.osm

import cats.syntax.all.*
import fs2.{Chunk, Stream}
import org.openstreetmap.osmosis.osmbinary.osmformat.{StringTable, DenseInfo, DenseNodes}
import java.time.Instant
import fs2.Pure

class DenseNodeStream[F[_]](stringTable: StringTable, latOffset: Long = 0, lonOffset: Long = 0, granularity: Int = 100) {
  def nodeIds(denseNodes: DenseNodes): Stream[F, Long] =
    streamIds(denseNodes.id)

  def nodes(denseNodes: DenseNodes): Stream[F, Node] =
    streamIds(denseNodes.id)
      .zip(streamCoordinates(denseNodes.lon zip denseNodes.lat))
      .zip(streamTags(denseNodes.keysVals)  zip streamInfo(denseNodes.denseinfo))
      .map { case ((id, coordinate), (tags, info)) => Node(id, coordinate, tags, info) }

  private def streamIds(ids: Seq[Long]) = Stream.emits(ids.tail).scan(ids.head) { _ + _ }

  private def streamCoordinates(lonLat: Seq[(Long, Long)]) =
    Stream
      .emits(lonLat.tail)
      .scan(toCoordinate(lonLat.head)) { addToCoordinate }

  private def streamTags(keysVals: Seq[Int]) =
    Stream
      .emits(keysVals)
      .split { _ == 0 }
      .map   { c => stringTable.tags(c.iterator) }

  private def toCoordinate(nanoLonLat: (Long, Long)) =
    Coordinate(
      longitude = .000000001 * (lonOffset + (granularity * nanoLonLat._1)),
      latitude  = .000000001 * (latOffset + (granularity * nanoLonLat._2))
    )

  private def addToCoordinate(coordinate: Coordinate, nanoLonLat: (Long, Long)) =
    Coordinate(
      longitude = coordinate.longitude + .000000001 * granularity * nanoLonLat._1,
      latitude  = coordinate.latitude  + .000000001 * granularity * nanoLonLat._2
    )

  private def streamInfo[F[_]](denseInfo: Option[DenseInfo]) = {
    val versions    = streamForever(denseInfo.toSeq.flatMap(_.version))
    val timestamps  = streamForever(denseInfo.toSeq.flatMap(_.timestamp))
    val changesets  = streamForever(denseInfo.toSeq.flatMap(_.changeset))
    val uids        = streamForever(denseInfo.toSeq.flatMap(_.uid))
    val userSids    = streamForever(denseInfo.toSeq.flatMap(_.userSid))
    val visibles    = streamForever(denseInfo.toSeq.flatMap(_.visible))

    (versions zip timestamps zip changesets zip uids zip userSids zip visibles).map {
      case (((((version, timestamp), changeset), uid), userSid), visible) =>
        fs2.osm.Info(
          version,
          timestamp.map { Instant.ofEpochSecond },
          changeset,
          uid,
          userSid.map { stringTable.getString },
          visible
        )
    }
  }

  private def streamForever[F[_], T](xs: Seq[T]) = Stream.emits(xs).map { _.some } ++ Stream.constant(Option.empty)
}
