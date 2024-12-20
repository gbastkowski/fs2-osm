package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import fs2.*
import net.postgis.jdbc.geometry.{LinearRing, LineString, MultiPolygon, Point, Polygon}

object PolygonFeature extends Feature {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] = PolygonFeature[F].apply(xa)

  override val tableDefinitions: List[Table] = List(
    Table("polygons",
          Column("id",          BigInt, GeneratedPrimaryKey),
          Column("way_id",      BigInt),
          Column("relation_id", BigInt),
          Column("name",        VarChar),
          Column("role",        VarChar),
          Column("tags",        Jsonb),
          Column("geom",        Geography(fs2.osm.postgres.Polygon, Wgs84)))
  )
}

class PolygonFeature[F[_]: Async] extends Queries {
  def apply(xa: Transactor[F]) =
    Stream
      .eval(taggedPolygons(xa))
      // .append(combineWays(xa))
      .map { "polygons" -> _ }

  private def taggedPolygons(xa: Transactor[F]) =
    logAndRun(
      sql"""
        INSERT INTO polygons	  (way_id, name, role, geom,                 tags)
        SELECT                   way_id, name, role, ST_MakePolygon(geom), tags
        FROM (
            SELECT  ways.osm_id                                     AS way_id,
                    ways.tags->>'name'                              AS name,
                    ways.tags->>'role'                              AS role,
                    ST_MakeLine(array_agg(nodes.geom)::geometry[])  AS geom,
                    ways.tags                                       AS tags
            FROM                ways
            CROSS JOIN LATERAL  unnest(ways.nodes)                  AS node_id
            INNER JOIN          nodes                               ON nodes.osm_id = node_id
            WHERE               (ways.tags->>'area' IS NULL         OR ways.tags->>'area' != 'no')
            AND                 (ways.tags ??'amenity'                                                                OR
                                 ways.tags ??'area:highway'                                                           OR
                                 ways.tags ??'building'                                                               OR
                                 ways.tags ??'boundary'                                                               OR
                                 ways.tags ??'building:part'                                                          OR
                                 ways.tags ??'historic'                                                               OR
                                 ways.tags ??'landuse'                                                                OR
                                 ways.tags ??'place'                                                                  OR
                                 ways.tags ??'shop'                                                                   OR
                                 ways.tags->>'waterway'              = 'riverbank'                                    OR
                                 ways.tags->>'highway'              IN ('rest_area','services', 'platform')           OR
                                 ways.tags->>'railway'              IN ('platform')                                   OR
                                 ways.tags->>'leisure'              IN ('picnic_table', 'slipway')                    OR
                                 ways.tags->>'leisure'              IN ('firepit')                                    OR
                                 ways.tags->>'natural'              IN ('water', 'wood', 'scrub', 'wetland',
                                                                        'grassland', 'heath', 'rock', 'bare_rock',
                                                                        'sand', 'beach', 'scree', 'bay', 'glacier',
                                                                        'shingle', 'fell', 'reef', 'stone', 'mud',
                                                                        'landslide', 'sinkhole', 'crevasse',
                                                                        'desert')                                     OR
                                 ways.tags->>'aeroway'              IN ('aerodrome'))
            GROUP BY            ways.osm_id
        ) AS grouped_nodes
        WHERE                   ST_IsClosed(geom)
      """
    ).transact(xa)

  case class CombinedLineString(relationId: Long, name: Option[String], role: Option[String], tags: Map[String, String], geom: Polygon)

  /*
   * Processes ways within a given relation, filtering and merging them into a polygon shape.
   * The function adheres to the following rules when gathering ways:
   *
   * - **Relation and Role Filtering:** Only ways that belong to the same relation and share the same role
   *   (e.g., "outer" or "inner") are collected. This ensures that only relevant ways contribute to the polygon.
   * - **Exclusion of Closed Ways:** Closed ways (those forming a loop independently) are excluded from the result,
   *   as they already define a standalone boundary.
   * - **Merging Open Ways:** Non-closed ways are combined (stitched together) to form a contiguous polygon.
   *   This merging allows for a complete representation of the area defined by the relation.
   */
  private def combineWays(xa: Transactor[F]): Stream[[x] =>> F[x], scala.Int] =
    sql"""
      SELECT  relation_id                               AS relation_id,
              name                                      AS name,
              role                                      AS role,
              tags                                      AS tags,
              CASE WHEN ST_IsRing((dump).geom)
                  THEN ST_MakePolygon((dump).geom)::geography
                  ELSE ST_MakePolygon(ST_AddPoint((dump).geom, ST_StartPoint((dump).geom)))::geography
              END                                       AS geom
      FROM (
            SELECT      relations.osm_id                AS relation_id,
                        relations.name                  AS name,
                        relations_ways.role             AS role,
                        relations.tags                  AS tags,
                        ST_Dump(ST_LineMerge(ST_Union(osm_lines.geom::geometry ORDER BY relations_ways.index))) AS dump
            FROM        relations
            INNER JOIN  relations_ways                  ON relations_ways.relation_id = relations.osm_id
            INNER JOIN  osm_lines                       ON osm_lines.osm_id           = relations_ways.way_id
            WHERE       relations.type = 'multipolygon'
            GROUP BY    relations.osm_id, relations_ways.role
      )
    """
      .query[CombinedLineString].stream.transact(xa)
      .flatMap { ls => Stream.eval(insert(ls.relationId, ls.name, ls.role, ls.tags, ls.geom).transact(xa)) }

  private def insert(relationId: Long, name: Option[String], role: Option[String], tags: Map[String, String], geom: Polygon) =
    logAndRun(
      sql"""
        INSERT INTO polygons	(relation_id, name, role, tags, geom)
        VALUES                ($relationId, $name, $role, $tags, $geom)
      """
    )
}
