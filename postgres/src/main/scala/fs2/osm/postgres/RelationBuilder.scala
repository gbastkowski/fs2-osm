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
      polygons    <- updatePolygons
    yield
      summary
        .update("ways")(waysUpdated)
        .update("polygons")(polygons)

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
