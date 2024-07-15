package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import core.{Node, OsmEntity}
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
import org.typelevel.otel4s.metrics.Counter
import org.typelevel.otel4s.Attribute

object NodeImporter:
  def apply[F[_]: Async](counter: Long => F[Unit], xa: Transactor[F]): Pipe[F, OsmEntity, (String, Int)] = _
    .collect { case n: Node     => n }
    .chunkN(10000, allowFewer = true)
    .map { handleNodes }
    .parEvalMap(20) { _.transact(xa) }
    .evalTap { counter(_) }
    .foldMonoid
    .map { "nodes" -> _ }

  private def handleNodes(chunk: Chunk[Node]) =
    val sql = "INSERT INTO nodes (osm_id, name, geom, tags) VALUES (?, ?, ?, ?)"
    Update[(Long, Option[String], Point, Json)](sql).updateMany(chunk.map { _.asTuple })

  extension (n: Node) def asTuple =
    (
      n.osmId,
      n.tags.get("name"),
      Point(n.coordinate.longitude, n.coordinate.latitude),
      toJson(n.tags)
    )

  private def toJson(tags: Map[String, String]) = Json.obj(tags.mapValues { _.asJson }.toSeq: _*)
