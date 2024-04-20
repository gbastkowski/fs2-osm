package fs2.osm
package postgres

import cats.*
import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.util.transactor.Transactor
import org.apache.logging.log4j.scala.Logging

class RelationBuilder[F[_]: Async](xa: Transactor[F]) extends Logging {
  def run(summary: Summary) =
    for
      waysUpdated <- (updateWays)
      highways    <- insertHighways.update.run.transact(xa)
      polygons    <- updatePolygons
    yield
      summary
        .update("ways")(waysUpdated)
        .insert("highways")(highways)
        .update("polygons")(polygons)

  private lazy val insertHighways = sql"""
    INSERT INTO highways	(osm_id, name, kind, footway, sidewalk, cycleway, busway, bicycle_road, surface, geom, tags)
    SELECT			 osm_id, name, kind, footway, sidewalk, cycleway, busway, bicycle_road, surface, geom, tags
    FROM (
        SELECT			ways.osm_id						AS osm_id,
      				ways.name						AS name,
      				ways.tags->>'highway'					AS kind,
      				ways.tags->>'footway'					AS footway,
      				ways.tags->>'sidewalk'					AS sidewalk,
      				ways.tags->>'cycleway'					AS cycleway,
      				ways.tags->>'busway'					AS busway,
      				ways.tags->>'bicycle_road' IS NOT NULL			AND
      				ways.tags->>'bicycle_road' = 'yes'			AS bicycle_road,
      				ways.tags->>'surface'					AS surface,
      				ST_MakeLine(array_agg(nodes.geom)::geometry[])		AS geom,
      				ways.tags						AS tags
        FROM			ways
        CROSS JOIN LATERAL	unnest(ways.nodes)					AS node_id
        INNER JOIN		nodes							ON nodes.osm_id = node_id
        WHERE			ways.tags->>'highway'					IS NOT NULL
        GROUP BY		ways.osm_id
    ) AS grouped_nodes
  """

  private lazy val updateWays =
    sql"""
      UPDATE ways
      SET    geom = subquery.geom
      FROM (
          SELECT id, ST_MakeLine(points::geometry[])   AS geom
          FROM (
              SELECT             ways.osm_id           AS id,
                                 array_agg(nodes.geom) AS points
              FROM               ways
              CROSS JOIN LATERAL unnest(ways.nodes)    AS node_id
              INNER JOIN         nodes                 ON nodes.osm_id = node_id
              GROUP BY           ways.osm_id
          ) AS grouped_nodes
      ) AS subquery
      WHERE osm_id = subquery.id
    """.update.run.transact(xa)

  private lazy val updatePolygons =
    sql"""
      UPDATE polygons
      SET    geom = subquery.geom
      FROM (
          SELECT id, ST_MakeLine(points::geometry[])   AS geom
          FROM (
              SELECT             ways.osm_id           AS id,
                                 array_agg(nodes.geom) AS points
              FROM               ways
              CROSS JOIN LATERAL unnest(ways.nodes)    AS node_id
              INNER JOIN         nodes                 ON nodes.osm_id = node_id
              GROUP BY           ways.osm_id
          ) AS grouped_nodes
      ) AS subquery
      WHERE osm_id = subquery.id
    """.update.run.transact(xa)
}
