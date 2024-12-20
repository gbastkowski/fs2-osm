package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.util.fragment.Fragment
import fs2.Stream

object HighwayFeature extends OptionalFeature with Queries {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] =
    List(
      Table("highways",
            Column("osm_id", BigInt, PrimaryKey),
            Column("name", VarChar),
            Column("kind", VarChar, NotNull("''")),
            Column("footway", VarChar),
            Column("sidewalk", VarChar),
            Column("cycleway", VarChar),
            Column("busway", VarChar),
            Column("bicycle_road", VarChar, NotNull("false")),
            Column("surface", VarChar),
            Column("tags", Jsonb),
            Column("geom", Geography(LineString, Wgs84)),
      ),
      Table("highways_nodes",
            Column("highway_id", BigInt, NotNull()),
            Column("node_id", BigInt, NotNull())),
    )

  private val dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "highways" -> logAndRun(sql"""
      INSERT INTO highways (osm_id, name, kind, footway, sidewalk, cycleway, busway, bicycle_road, surface, geom, tags)
      SELECT                osm_id, name, kind, footway, sidewalk, cycleway, busway, bicycle_road, surface, geom, tags
      FROM (
          SELECT
              ways.osm_id AS osm_id,
              ways.name   AS name,
              ways.tags->>'highway' AS kind,
              ways.tags->>'footway' AS footway,
              ways.tags->>'sidewalk' AS sidewalk,
              ways.tags->>'cycleway' AS cycleway,
              ways.tags->>'busway' AS busway,
              ways.tags->>'bicycle_road' IS NOT NULL AND ways.tags->>'bicycle_road' = 'yes' AS bicycle_road,
              ways.tags->>'surface' AS surface,
              ST_MakeLine(array_agg(nodes.geom)::geometry[]) AS geom,
              ways.tags AS tags
          FROM ways
          CROSS JOIN LATERAL unnest(ways.nodes) AS node_id
          INNER JOIN nodes ON nodes.osm_id = node_id
          WHERE ways.tags->>'highway' IS NOT NULL
          GROUP BY ways.osm_id
      ) AS grouped_nodes
    """)
  )
}
