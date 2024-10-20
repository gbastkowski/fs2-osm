package fs2.osm
package postgres

import cats.effect.Async
import cats.free.Free
import cats.syntax.all.*
import doobie.*
import doobie.free.ConnectionIO
import doobie.free.connection.ConnectionOp
import doobie.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import doobie.util.query.Query0
import net.postgis.jdbc.geometry.{LinearRing, LineString, MultiPolygon, Polygon}
import fs2.{Chunk, Stream}

// TODO test outer polygon with multiple ways, see relation 3352832
trait MultiPolygonBuilder {
  type Name     = String
  type Tags     = Map[String, String]
  type Relation = (Long, Option[Name], Tags)

  case class Record(osmId: Long, name: Option[String], tags: Map[String, String], geom: MultiPolygon)

  def findMultiPolygonsByTag(key: String, value: String): Stream[[x] =>> ConnectionIO[x], Record] =
    for
      (relationId, name, tags) <- findMultiPolygonRelations.stream
      multiPolygon             <- findMultiPolygonByRelationId(relationId)
    yield Record(relationId, name, tags, multiPolygon)

  private def findMultiPolygonByRelationId(relationId: Long) = {
    val outerRing  = findOuterRingByRelationId(relationId).stream
    val innerRings = findInnerRingsByRelationId(relationId).stream

    (outerRing ++ innerRings).chunkAll.map { chunk => MultiPolygon(Array(Polygon(chunk.toArray))) }
  }

  private val findMultiPolygonRelations: Query0[Relation] = sql"""
      SELECT      osm_id, name, tags
      FROM        relations
      WHERE       relations.type               = 'multipolygon'
      AND         relations.tags->>'natural'   = 'water'
  """.query[Relation]

  private def findInnerWaysByRelationId(relationId: Long) = sql"""
      SELECT      way_id
      FROM        relations_ways
      WHERE       relation_id  = $relationId
      AND         role = 'inner'
      ORDER BY    index
  """.query[Long]

  private def findPolygonByWayId(wayId: Long) = sql"""
      SELECT CASE WHEN ST_IsClosed(geom)
               THEN ST_MakePolygon(geom)::geography
               ELSE ST_MakePolygon(ST_AddPoint(geom, ST_StartPoint(geom)))::geography
             END AS geom
      FROM (
        SELECT  ST_MakeLine(geom::geometry)              AS geom,
                ST_IsClosed(ST_MakeLine(geom::geometry)) AS closed
        FROM (
            SELECT      geom
            FROM        nodes
            INNER JOIN  ways_nodes  ON ways_nodes.node_id = nodes.osm_id
            WHERE       way_id       = $wayId
            ORDER BY    index
        )
      )
  """.query[Polygon]

  // Create polygon of all nodes in a given relation
  private def findInnerRingsByRelationId(relationId: Long) = sql"""
      SELECT CASE WHEN ST_IsClosed(geom)
               THEN   geom::geography
               ELSE   ST_AddPoint(geom, ST_StartPoint(geom))::geography
             END   AS geom
      FROM (
        SELECT        way_id, role,
                      count(geom)                              AS num_points,
                      ST_MakeLine(geom::geometry)              AS geom,
                      ST_IsClosed(ST_MakeLine(geom::geometry)) AS closed
        FROM (
          SELECT      ways_nodes.way_id, role, geom
          FROM        nodes
          INNER JOIN  ways_nodes     ON ways_nodes.node_id      = nodes.osm_id
          INNER JOIN  relations_ways ON relations_ways.way_id   = ways_nodes.way_id
          WHERE       relation_id = $relationId
          AND         role = 'inner'
          ORDER BY    relations_ways.index, ways_nodes.index
        )
        GROUP BY way_id, role
      )
      WHERE num_points > 3
  """.query[LineString].map(ls => LinearRing(ls.getPoints))

  // Create polygon of all nodes in a given relation
  private def findOuterRingByRelationId(relationId: Long) = sql"""
      SELECT CASE WHEN ST_IsClosed(geom)
               THEN   geom::geography
               ELSE   ST_AddPoint(geom, ST_StartPoint(geom))::geography
             END   AS geom
      FROM (
        SELECT        ST_MakeLine(geom::geometry)              AS geom,
                      ST_IsClosed(ST_MakeLine(geom::geometry)) AS closed
        FROM (
          SELECT      geom
          FROM        nodes
          INNER JOIN  ways_nodes     ON ways_nodes.node_id      = nodes.osm_id
          INNER JOIN  relations_ways ON relations_ways.way_id   = ways_nodes.way_id
          WHERE       relation_id = $relationId
          AND         role = 'outer'
          ORDER BY    relations_ways.index, ways_nodes.index
        )
      )
  """.query[LineString].map(ls => LinearRing(ls.getPoints))

  // Create polygon of all nodes in a given relation
  private def findInnerPolygonsByRelationId(relationId: Long) = sql"""
      SELECT CASE WHEN ST_IsClosed(geom)
               THEN   ST_MakePolygon(geom)::geography
               ELSE   ST_MakePolygon(ST_AddPoint(geom, ST_StartPoint(geom)))::geography
             END   AS geom
      FROM (
        SELECT        way_id, role,
                      count(geom)                              AS num_points,
                      ST_MakeLine(geom::geometry)              AS geom,
                      ST_IsClosed(ST_MakeLine(geom::geometry)) AS closed
        FROM (
          SELECT      ways_nodes.way_id, role, geom
          FROM        nodes
          INNER JOIN  ways_nodes     ON ways_nodes.node_id      = nodes.osm_id
          INNER JOIN  relations_ways ON relations_ways.way_id   = ways_nodes.way_id
          WHERE       relation_id = $relationId
          AND         role = 'inner'
          ORDER BY    relations_ways.index, ways_nodes.index
        )
        GROUP BY way_id, role
      )
      WHERE num_points > 3
  """.query[Polygon]

  // Create polygon of all nodes in a given relation
  private def findOuterPolygonByRelationId(relationId: Long) = sql"""
      SELECT CASE WHEN ST_IsClosed(geom)
               THEN   ST_MakePolygon(geom)::geography
               ELSE   ST_MakePolygon(ST_AddPoint(geom, ST_StartPoint(geom)))::geography
             END   AS geom
      FROM (
        SELECT        ST_MakeLine(geom::geometry)              AS geom,
                      ST_IsClosed(ST_MakeLine(geom::geometry)) AS closed
        FROM (
          SELECT      geom
          FROM        nodes
          INNER JOIN  ways_nodes     ON ways_nodes.node_id      = nodes.osm_id
          INNER JOIN  relations_ways ON relations_ways.way_id   = ways_nodes.way_id
          WHERE       relation_id = $relationId
          AND         role = 'outer'
          ORDER BY    relations_ways.index, ways_nodes.index
        )
      )
  """.query[Polygon]
}
