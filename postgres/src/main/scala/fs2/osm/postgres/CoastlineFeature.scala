package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.util.fragment.Fragment
import fs2.Stream

object CoastlineFeature extends Feature with Queries {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] =
    List(
      Table("coastlines",
            Column("osm_id", BigInt, PrimaryKey),
            Column("tags", Jsonb),
            Column("geom", Geography(LineString, Wgs84))))

  private def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "coastlines" -> logAndRun(sql"""
      INSERT INTO coastlines (osm_id, geom, tags)
      SELECT                  osm_id, geom, tags
      FROM (
          SELECT
              ways.osm_id AS osm_id,
              ST_MakeLine(array_agg(nodes.geom)::geometry[]) AS geom,
              ways.tags AS tags
          FROM ways
          CROSS JOIN LATERAL unnest(ways.nodes) AS node_id
          INNER JOIN nodes ON nodes.osm_id = node_id
          WHERE ways.tags->>'natural' = 'coastline'
          GROUP BY ways.osm_id
      )
    """)
  )
}
