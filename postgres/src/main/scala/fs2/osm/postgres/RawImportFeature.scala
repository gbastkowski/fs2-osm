package fs2.osm.postgres

import cats.effect.Async
import doobie.*
import doobie.implicits.*
import fs2.Stream
import io.circe.Json
import org.postgis.Point
import scala.io.Source

class RawImportFeature[F[_]: Async](in: Stream[F, (Long, String, Point, Json)]) extends Feature {
  override val name: String = "asdf"

  override val tableDefinitions: List[Table] = List()

  override val dataGenerator: List[Fragment] =
    Nil
    // Update(
    //   """
    //     INSERT INTO nodes (osm_id, name, geom, tags)
    //     VALUES            (?, ?, ?, ?)
    //   """
    // ).updateMany[(Long, String, Point, Json)](in.compile.toList)

    List(
    sql"""
      INSERT INTO waters	    (osm_id, name, kind, geom,                 tags)
      SELECT                   osm_id, name, kind, ST_MakePolygon(geom), tags
      FROM (
          SELECT
              ways.osm_id   AS osm_id,
              ways.name     AS name,
              ways.tags->>'water' AS kind,
              ST_MakeLine(array_agg(nodes.geom)::geometry[]) AS geom,
              ways.tags AS tags
          FROM ways
          CROSS JOIN LATERAL unnest(ways.nodes) AS node_id
          INNER JOIN nodes  nodes.osm_id = node_id
          WHERE ways.tags->>'natural' = 'water'
          GROUP BY ways.osm_id
      ) AS grouped_nodes
      WHERE ST_IsClosed(geom)
    """
  )
}
