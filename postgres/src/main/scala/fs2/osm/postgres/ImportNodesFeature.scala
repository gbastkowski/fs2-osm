package fs2.osm
package postgres

import cats.effect.Async
import doobie.*
import doobie.implicits.*
import fs2.{Chunk, Stream}
import fs2.osm.core.Node
import io.circe.*
import io.circe.syntax.*

import scala.io.Source
import cats.free.Free
import doobie.free.connection.ConnectionOp

object ImportNodesFeature extends Feature {
  override val name: String = "nodes"

  override val tableDefinitions: List[Table] = List(
    Table("nodes")
      .withColumns(
        Column("osm_id",  BigInt,                   PrimaryKey),
        Column("name",    VarChar),
        Column("geom",    Geography(Point, Wgs84),  NotNull()),
        Column("tags",    Jsonb,                    NotNull()))
  )

  override val dataGenerator: List[Fragment] = List(
    sql"INSERT INTO nodes (osm_id, name, geom, tags) VALUES (?, ?, ?, ?)"
  )
}

class ImportNodesFeature[F[_]: Async](nodes: Stream[F, Node]) {
  def run: Stream[F, Free[ConnectionOp, scala.Int]] =
    nodes
      .chunkN(10000, allowFewer = true)
      .map { n => handleNodes(n) }

  private def handleNodes(chunk: Chunk[Node]) = {
    val tuples = chunk.map { n =>
      (
        n.osmId,
        n.tags.get("name"),
        org.postgis.Point(n.coordinate.longitude, n.coordinate.latitude),
        toJson(n.tags)
      )
    }
    Update[(Long, Option[String], org.postgis.Point, Json)](ImportNodesFeature.dataGenerator.head.update.sql)
      .updateMany(tuples)
  }

  private def toJson(tags: Map[String, String]) = Json.obj(tags.mapValues { _.asJson }.toSeq: _*)
}
