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
  def run(summary: Summary): F[Summary] =
    Seq(
      insertOsmObject("highways", insertHighways, summary),
      insertOsmObject("railways", insertRailways, summary),
      insertOsmObject("buildings", insertBuildings, summary),
      insertOsmObject("protected_areas", insertProtectedAreas, summary),
    )
      .sequence
      .map { _.combineAll }

    // for
    //   highways    <- insertHighways.update.run.transact(xa)
    //   railways    <- insertRailways.update.run.transact(xa)
    //   buildings   <- insertBuildings.update.run.transact(xa)
    // yield
    //   summary
    //     .insert("highways")(highways)
    //     .insert("railways")(railways)
    //     .insert("buildings")(buildings)

  private def insertOsmObject(summaryKey: String, sql: Fragment, summary: Summary) =
    sql.update.run.transact(xa).map { summary.insert(summaryKey) }

  private lazy val insertRailways = sql"""
    INSERT INTO railways	(osm_id, name, official_name, operator, geom, tags)
    SELECT			 osm_id, name, official_name, operator, geom, tags
    FROM (
        SELECT			ways.osm_id						AS osm_id,
      				ways.name						AS name,
      				ways.tags->>'official_name'				AS official_name,
      				ways.tags->>'operator'					AS operator,
      				ST_MakePolygon(ST_MakeLine(array_agg(nodes.geom)::geometry[]))		AS geom,
      				ways.tags						AS tags
        FROM			ways
        CROSS JOIN LATERAL	unnest(ways.nodes)					AS node_id
        INNER JOIN		nodes							ON nodes.osm_id = node_id
        WHERE			ways.tags->>'landuse' = 'railway'
        GROUP BY		ways.osm_id
    ) AS grouped_nodes
  """

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

  private lazy val insertBuildings = sql"""
    INSERT INTO buildings	(osm_id, name, kind, geom, tags)
    SELECT			 osm_id, name, kind, geom, tags
    FROM (
        SELECT			ways.osm_id						AS osm_id,
      				ways.name						AS name,
      				ways.tags->>'building'					AS kind,
      				ST_MakePolygon(ST_MakeLine(array_agg(nodes.geom)::geometry[]))		AS geom,
      				ways.tags						AS tags
        FROM			ways
        CROSS JOIN LATERAL	unnest(ways.nodes)					AS node_id
        INNER JOIN		nodes							ON nodes.osm_id = node_id
        WHERE			ways.tags->>'building'					IS NOT NULL
        GROUP BY		ways.osm_id
    ) AS grouped_nodes
  """

  private lazy val insertProtectedAreas = sql"""
    INSERT INTO protected_areas (osm_id, name, kind, geom,                 tags)
    SELECT			 osm_id, name, kind, ST_MakePolygon(geom), tags
    FROM (
        SELECT			ways.osm_id						AS osm_id,
      				ways.name						AS name,
      				ways.tags->>'protection_title'				AS kind,
      				ST_MakeLine(array_agg(nodes.geom)::geometry[])		AS geom,
      				ways.tags						AS tags
        FROM			ways
        CROSS JOIN LATERAL	unnest(ways.nodes)					AS node_id
        INNER JOIN		nodes							ON nodes.osm_id = node_id
        WHERE			ways.tags->>'boundary' = 'protected_area'
        GROUP BY		ways.osm_id
    ) AS grouped_nodes
    WHERE ST_IsClosed(geom)
  """
}
