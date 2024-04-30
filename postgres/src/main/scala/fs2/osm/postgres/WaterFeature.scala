package fs2.osm.postgres

import doobie.*
import doobie.implicits.*
import scala.io.Source

object WaterFeature extends Feature {
  override val name: String = "waters"

  override val tableDefinitions: List[Table] = List(
    Table("waters",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("kind", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(Polygon, Wgs84))),
    Table("waters_nodes",
          Column("water_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull())),
  )

  override val dataGenerator: List[Fragment] = List(
    sql"""
      INSERT INTO waters	    (osm_id, name, kind, geom,                 tags)
      SELECT                   osm_id, name, kind, ST_MakePolygon(geom), tags
      FROM (
          SELECT  ways.osm_id                                       AS osm_id,
                  ways.name                                         AS name,
                  ways.tags->>'water'                               AS kind,
                  ST_MakeLine(array_agg(nodes.geom)::geometry[])    AS geom,
                  ways.tags                                         AS tags
          FROM                ways
          CROSS JOIN LATERAL  unnest(ways.nodes)                    AS node_id
          INNER JOIN          nodes                                 ON nodes.osm_id = node_id
          WHERE               ways.tags->>'natural' = 'water'
          GROUP BY            ways.osm_id
      ) AS grouped_nodes
      WHERE                   ST_IsClosed(geom)
    """
  )
}
