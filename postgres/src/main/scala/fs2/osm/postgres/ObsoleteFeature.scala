package fs2.osm
package postgres

import cats.effect.Async
import cats.free.Free
import cats.syntax.all.*
import doobie.*
import doobie.free.ConnectionIO
import doobie.free.connection.ConnectionOp
import doobie.implicits.*
import doobie.postgres.circe.json.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import doobie.util.query.Query0
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import net.postgis.jdbc.geometry.*
import org.postgresql.util.PGobject
import scala.io.Source

object _ObsoleteFeature extends Feature {
  type Water = (Long, Option[String], Option[String], Polygon, Map[String, String])

  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] = List(
    Table("waters",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("kind", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(fs2.osm.postgres.MultiPolygon, Wgs84))),
    Table("waters_nodes",
          Column("water_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull())),
  )

  private val dataGenerator: List[(String, ConnectionIO[Int])] =
    val simpleWaters = logAndRun(
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
      """)
    List("waters" -> simpleWaters)

  // =================================================================================
  // === Unused stuff  ===============================================================
  // =================================================================================

  private def _insertList(waters: List[Water]) = Update[Water]("""
      INSERT  INTO waters (osm_id, name, kind, geom, tags)
      VALUES  (?, ?, ?, ?, ?)
  """).updateMany(waters)

  private def _insert[F[_]: Async](xa: Transactor[F]): Stream[F, Int] = {
    val sql = "INSERT INTO waters (osm_id, name, kind, geom, tags) values (?, ?, ?, ?, ?)"
    val found: Stream[ConnectionIO, Water] =
      for
        (relationId, name, kind, tags) <- multiPolygons.stream
        outerPolygon                   <- _outerComplexPolygon(relationId).stream // ++
                                          // outerSimplePolygon(relationId).stream
        water                           = (relationId, name, tags.get("water"), outerPolygon, tags)
      yield (relationId, name, tags.get("water"), outerPolygon, tags)

    found
      .chunks
      .flatMap { chunk => Stream.eval(Update[Water](sql).updateMany(chunk)) }
      .transact(xa)
  }

  private def _outerComplexPolygon(relationId: Long): Query0[Polygon] =
    sql"""
      SELECT  CASE WHEN ST_IsClosed(geom)
                  THEN ST_MakePolygon(geom)::geography
                  ELSE ST_MakePolygon(ST_AddPoint(geom, ST_StartPoint(geom)))::geography
              END AS geom
      FROM (
          SELECT ST_LineMerge(ST_Union(geom)) AS geom
          FROM (
              SELECT  way_id,
                      ST_MakeLine(geom::geometry) as geom
              FROM (
                  SELECT      way_id, geom
                  FROM        nodes n
                  INNER JOIN  ways_nodes wn   ON wn.node_id = n.osm_id
                  WHERE way_id IN (
                      SELECT  way_id
                      FROM    relations_ways
                      WHERE   relation_id = $relationId
                      AND     role = 'outer')
              ) as points
              GROUP BY way_id
          ) AS lines
      ) as merged
      WHERE ST_GeometryType(geom) = 'ST_LineString'
    """.query[Polygon]

  private val multiPolygons: Query0[(Long, Option[String], Option[String], Map[String, String])] = sql"""
      SELECT  osm_id,
              name,
              relations.tags->>'water'    as kind,
              tags
      FROM    relations
      WHERE   relations.type               = 'multipolygon'
      AND     relations.tags->>'natural'   = 'water'
  """.query[(Long, Option[String], Option[String], Map[String, String])]

  private def _outerSimplePolygon(relationId: Long) =
    sql"""
      SELECT ST_MakePolygon(geom)::geography geom
      FROM (
          SELECT  way_id,
                  ST_IsClosed(ST_MakeLine(geom::geometry)) as closed,
                  ST_MakeLine(geom::geometry) as geom
          FROM (
              SELECT      way_id, geom
              FROM        nodes n
              INNER JOIN  ways_nodes wn ON wn.node_id = n.osm_id
              WHERE way_id IN (
                  SELECT  way_id
                  FROM    relations_ways
                  WHERE   relation_id = $relationId
                  AND     role = 'outer')
          ) as points
          GROUP BY way_id
      ) lines
      WHERE closed
    """.query[Polygon]

  private def _mergedLineStrings(relationId: Long, role: String) =
    sql"""
      SELECT ST_MakeLine(array_agg(geom))
      FROM (
          SELECT      n.geom::geometry
          FROM        relations_ways rw
          INNER JOIN  ways           w  ON rw.way_id  = w.osm_id
          INNER JOIN  ways_nodes     wn ON wn.way_id  = w.osm_id
          INNER JOIN  nodes          n  ON wn.node_id = n.osm_id
          ORDER BY    wn.index
      ) AS lines
  """

  private def _lineString(wayId: Long) =
    sql"""
      SELECT ST_MakeLine(array_agg(geom))
      FROM (
          SELECT      n.geom::geometry
          FROM        ways_nodes     wn
          INNER JOIN  nodes          n  ON node_id = n.osm_id
          WHERE       way_id = $wayId
          ORDER BY    wn.index
      ) AS nodes
    """.query[LineString]

  private def _ways(relationId: Long, role: String) =
    sql"""
      SELECT      way_id
      FROM        relations_ways
      WHERE       relation_id = $relationId
      ORDER BY    index
    """.query[Long]

  private def logAndRun(sql: Fragment): ConnectionIO[Int] = {
    logger.debug(sql.update.sql)
    sql.update.run
  }
}
