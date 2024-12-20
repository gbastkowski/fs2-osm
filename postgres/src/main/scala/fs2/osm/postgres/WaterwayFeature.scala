package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.util.fragment.Fragment
import fs2.Stream

object WaterwayFeature extends OptionalFeature with Queries {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] =
    List(
      Table("waterways",
            Column("osm_id", BigInt, PrimaryKey),
            Column("name", VarChar),
            Column("kind", VarChar, NotNull("''")),
            Column("tags", Jsonb),
            Column("geom", Geography(LineString, Wgs84)),
      ),
      Table("waterways_nodes",
            Column("waterway_id", BigInt, NotNull()),
            Column("node_id", BigInt, NotNull())),
    )

  private def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "waterways" -> logAndRun(sql"""
      INSERT INTO waterways (osm_id, name, kind, geom, tags)
      SELECT                 osm_id, name, kind, geom, tags
      FROM (
          SELECT
              ways.osm_id AS osm_id,
              ways.name   AS name,
              ways.tags->>'waterway' AS kind,
              ST_MakeLine(array_agg(nodes.geom)::geometry[]) AS geom,
              ways.tags AS tags
          FROM ways
          CROSS JOIN LATERAL unnest(ways.nodes) AS node_id
          INNER JOIN nodes ON nodes.osm_id = node_id
          WHERE ways.tags->>'waterway' IS NOT NULL
          GROUP BY ways.osm_id
      ) AS grouped_nodes
    """)
  )
}
