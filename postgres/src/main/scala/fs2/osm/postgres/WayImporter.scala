package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import core.{OsmEntity, Way}
import doobie.*
import doobie.implicits.*
import doobie.implicits.javatimedrivernative.*
import doobie.postgres.*
import doobie.postgres.circe.json.implicits.*
import doobie.postgres.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import doobie.postgres.pgisimplicits.*
import fs2.{Chunk, Pipe}
import io.circe.Json
import io.circe.syntax.*
import org.postgis.Point

object WayImporter {
  def apply[F[_]: Async](xa: Transactor[F]): Pipe[F, OsmEntity, (String, Int)] = _
    .collect { case n: Way      => n }
    .chunkN(10000, allowFewer = true)
    .map { handleWays }
    .evalMap { _.transact(xa) }
    .map { "ways" -> _ }

  private def handleWays(chunk: Chunk[Way]) = {
    val ways = chunk.map { w =>
      (
        w.osmId,
        w.tags.get("name"),
        w.nodes.toArray,
        toJson(w.tags)
      )
    }

    val nodesWithIndex = chunk.toList.flatMap { way => way.nodes.zipWithIndex.map { (nodeId, index) => (way.osmId, nodeId, index) } }

    Seq(
      Update[(Long, Option[String], Array[Long], Json)]("INSERT INTO ways (osm_id, name, nodes, tags) VALUES (?, ?, ?, ?)").updateMany(ways),
      Update[(Long, Long, Int)]("INSERT INTO ways_nodes (way_id, node_id, index) VALUES (?, ?, ?)").updateMany(nodesWithIndex)
    ).combineAll
  }

  private def toJson(tags: Map[String, String]) = Json.obj(tags.mapValues { _.asJson }.toSeq: _*)
}
