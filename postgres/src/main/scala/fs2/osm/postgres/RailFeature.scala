package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import fs2.Stream

object RailFeature extends Feature with Queries {
  def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] =
    List(
      Table("rails",
            Column("osm_id", BigInt, PrimaryKey),
            Column("kind", VarChar, NotNull("''")),
            Column("electrified", VarChar),
            Column("maxspeed", VarChar),
            Column("ref", VarChar),
            Column("tags", Jsonb),
            Column("geom", Geography(LineString, Wgs84)),
      ),
      Table("rails_nodes",
            Column("rail_id", BigInt, NotNull()),
            Column("node_id", BigInt, NotNull())),
    )

  private def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "highways" -> logAndRun(sql"""
      INSERT INTO rails (osm_id, kind, electrified, maxspeed, ref, geom, tags)
      SELECT             osm_id, kind, electrified, maxspeed, ref, geom, tags
      FROM (
          SELECT
              ways.osm_id                AS osm_id,
              ways.tags->>'railway'      AS kind,
              ways.tags->>'electrified'  AS electrified,
              ways.tags->>'maxspeed'     AS maxspeed,
              ways.tags->>'ref'          AS ref,
              ST_MakeLine(array_agg(nodes.geom)::geometry[]) AS geom,
              ways.tags AS tags
          FROM ways
          CROSS JOIN LATERAL unnest(ways.nodes) AS node_id
          INNER JOIN nodes ON nodes.osm_id = node_id
          WHERE ways.tags->>'railway' IS NOT NULL
          GROUP BY ways.osm_id
      ) AS grouped_nodes
    """)
  )
}
