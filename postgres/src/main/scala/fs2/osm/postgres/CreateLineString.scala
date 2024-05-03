package fs2.osm.postgres

object CreateLineString {
  val sql =
    """
      SELECT  way_id,
              count(points.way_id),
              ST_MakeLine(points.geom::geometry) AS geom
      FROM    ways w
      INNER JOIN (
        SELECT      w.osm_id AS way_id,
                    wn.index,
                    n.geom
        FROM        ways w
        INNER JOIN  ways_nodes wn   ON wn.way_id  = w.osm_id
        INNER JOIN  nodes      n    ON wn.node_id = n.osm_id
        ORDER BY    wn.index
      ) AS points ON points.way_id = w.osm_id
      GROUP BY  way_id
    """
}
