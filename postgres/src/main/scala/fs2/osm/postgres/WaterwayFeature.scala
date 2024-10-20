package fs2.osm
package postgres

import doobie.*
import doobie.implicits.*
import doobie.util.fragment.Fragment

object WaterwayFeature extends SqlFeature {
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

  override def dataGenerator: List[(String, ConnectionIO[Int])] = List(
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
