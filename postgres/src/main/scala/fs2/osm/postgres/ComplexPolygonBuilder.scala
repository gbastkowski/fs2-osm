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
import fs2.{Chunk, Stream}
import net.postgis.jdbc.geometry.{LinearRing, LineString, MultiPolygon, Polygon}

object ComplexPolygonBuilder extends ComplexPolygonBuilder

// TODO test outer polygon with multiple ways, see relation 3352832
trait ComplexPolygonBuilder {
  import Fragments.*

  case class Relation(id: Long, name: Option[String], tags: Map[String, String])

  object Record {
    def apply(r: Relation, m: MultiPolygon): Record = Record(r.id, r.name, r.tags, m)
  }
  case class Record(osmId: Long, name: Option[String], tags: Map[String, String], geom: MultiPolygon)
  val toRecord: (Relation, MultiPolygon) => Record = (r, m) => Record(r.id, r.name, r.tags, m)

  object RecordWithKind {
    def apply(r: Relation, m: MultiPolygon): RecordWithKind = RecordWithKind(r.id, r.name, r.tags.get("kind"), r.tags, m)
  }
  case class RecordWithKind(
    osmId: Long,
    name: Option[String],
    kind: Option[String],
    tags: Map[String, String],
    geom: MultiPolygon)

  def findMultiPolygonsByTag[T](key: String, value: String)(fn: (Relation, MultiPolygon) => T): Stream[[x] =>> ConnectionIO[x], T] =
    findMultiPolygonsByFragment(TagsFragments.is(key, value)) { fn }

  def findMultiPolygonsByEitherTag[T](t1: (String, String), t2: (String, String))(fn: (Relation, MultiPolygon) => T): Stream[[x] =>> ConnectionIO[x], T] =
    findMultiPolygonsByFragment(TagsFragments.or(t1, t2)) { fn }

  def findMultiPolygonsByTags[T](t1: (String, String), t2: (String, String))(fn: (Relation, MultiPolygon) => T): Stream[[x] =>> ConnectionIO[x], T] =
    findMultiPolygonsByFragment(TagsFragments.and(t1, t2)) { fn }

  def findMultiPolygonsWithTag[T](key: String)(fn: (Relation, MultiPolygon) => T): Stream[[x] =>> ConnectionIO[x], T] =
    findMultiPolygonsByFragment(TagsFragments.has(key)) { fn }

  def findMultiPolygonsByFragment[T](fragment: Fragment)(fn: (Relation, MultiPolygon) => T): Stream[[x] =>> ConnectionIO[x], T] =
    for
      relation     <- findMultiPolygonRelations(fragment).stream
      multiPolygon <- findMultiPolygonByRelationId(relation.id)
    yield fn(relation, multiPolygon)

  private def findMultiPolygonByRelationId(relationId: Long): Stream[[x] =>> ConnectionIO[x], MultiPolygon] =
    val outerPolygons = findWaysByRelationId(relationId, "outer")
      .stream
      .fold(List.empty[LineString]) { (acc, current) =>
        acc match {
          case head :: tail  => head.merge(current).fold(current :: acc)(_ :: tail)
          case Nil           => List(current)
        }
      }
      .flatMap { Stream.emits }
      .filter  { ls => ls.getFirstPoint == ls.getLastPoint }
      .map     { ls => Polygon(Array(LinearRing(ls.getPoints))) }
      .chunkAll
      .map { _.toArray }

    // org.locationtech.jts.geom.Polygon



    val outerRing  = findCombinedRingByRelationId(relationId, "outer").stream
    val innerRings = findSeparateRingsByRelationId(relationId, "inner").stream

    (outerRing ++ innerRings).chunkAll.map { chunk => MultiPolygon(Array(Polygon(chunk.toArray))) }



  private def findMultiPolygonRelations(fragment: Fragment): Query0[Relation] =
    val sql = fr"SELECT osm_id, name, tags FROM relations" ++ whereAnd(fr"relations.type = 'multipolygon'", fragment)
    sql.query[Relation]

  // Create polygon of all nodes in a given relation
  private def findCombinedRingByRelationId(relationId: Long, role: String) = sql"""
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
          AND         role        = $role
          ORDER BY    relations_ways.index, ways_nodes.index
        )
      )
  """.query[LineString].map { ls => LinearRing(ls.getPoints) }

  private def findSeparateRingsByRelationId(relationId: Long, role: String) = sql"""
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
          AND         role        = $role
          ORDER BY    relations_ways.index, ways_nodes.index
        )
        GROUP BY way_id, role
      )
      WHERE num_points > 3
  """.query[LineString].map { ls => LinearRing(ls.getPoints) }

  private def findWaysByRelationId(relationId: Long, role: String) = sql"""
      SELECT          geom
      FROM (
        SELECT        way_id, role, ST_MakeLine(geom::geometry) AS geom
        FROM (
          SELECT      ways_nodes.way_id, role, geom
          FROM        nodes
          INNER JOIN  ways_nodes     ON ways_nodes.node_id      = nodes.osm_id
          INNER JOIN  relations_ways ON relations_ways.way_id   = ways_nodes.way_id
          WHERE       relation_id = $relationId
          AND         role        = $role
          ORDER BY    relations_ways.index, ways_nodes.index
        )
        GROUP BY      way_id, role
      )
  """.query[LineString]

  // =================================================================================
  // === Unused stuff  ===============================================================
  // =================================================================================

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
