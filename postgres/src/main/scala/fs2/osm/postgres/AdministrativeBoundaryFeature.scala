package fs2.osm
package postgres

import cats.effect.Async
import cats.syntax.all.*
import doobie.*
import doobie.free.ConnectionIO
import doobie.implicits.*
import doobie.postgres.pgisimplicits.*
import doobie.util.query.Query0
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import net.postgis.jdbc.geometry.*
import org.postgresql.util.PGobject
import scala.io.Source
import doobie.free.connection.ConnectionOp
import cats.free.Free

object AdministrativeBoundaryFeature extends Feature {
  type AdministrativeBoundary = (Long, Option[String], Option[Int], MultiLineString, Map[String, String])

  override val tableDefinitions: List[Table] = List(
    Table("administrative_boundaries",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("admin_level", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(fs2.osm.postgres.MultiLineString, Wgs84))),
    )

  override val dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "administrative_boundaries" -> logAndRun(
      sql"""
        INSERT INTO administrative_boundaries (osm_id, name, admin_level, geom,          tags)
        SELECT                                 osm_id, name, admin_level, ST_Make(geom), tags
        FROM (
            SELECT  ways.osm_id                                       AS osm_id,
                    ways.name                                         AS name,
                    ways.tags->>'admin_level'                         AS admin_level,
                    ST_MakeLine(array_agg(nodes.geom)::geometry[])    AS geom,
                    ways.tags                                         AS tags
            FROM                ways
            CROSS JOIN LATERAL  unnest(ways.nodes)                    AS node_id
            INNER JOIN          nodes                                 ON nodes.osm_id = node_id
            WHERE               ways.tags->>'boundary' = 'administrative'
            GROUP BY            ways.osm_id
        ) AS grouped_nodes
        WHERE                   ST_IsClosed(geom)
      """
    )
  )

  def insert[F[_]: Async](xa: Transactor[F]): Stream[F, Int] = {
    val sql = "INSERT INTO administrative_boundaries (osm_id, name, admin_level, geom, tags) values (?, ?, ?, ?, ?)"
    val found: Stream[ConnectionIO, AdministrativeBoundary] =
      for
        (relationId, name, tags) <- boundaryRelations.stream
        outerLine                <- outerComplexLine(relationId).stream
        administrative_boundary   = (relationId, name, tags.get("admin_level").flatMap(_.toIntOption), outerLine, tags)
      yield administrative_boundary

    found
      .chunks
      .flatMap { chunk => Stream.eval(Update[AdministrativeBoundary](sql).updateMany(chunk)) }
      .transact(xa)
  }

  private val boundaryRelations =
    // RelationImporter.find("boundary", "administrative")
    sql"""
      SELECT  osm_id, name, tags
      FROM    relations
      WHERE   relations.type = 'boundary'
      AND     relations.tags->>'boundary' = 'administrative'
    """
      .query[(Long, Option[String], Map[String, String])]

  private def outerComplexLine(relationId: Long) =
    sql"""
      SELECT      ST_Collect(w.geom)::geography
      FROM        relations rl
      INNER JOIN  relations_ways rw ON rw.relation_id = r.osm_id
      INNER JOIN (
          SELECT  way_id,
                  count(points.way_id),
                  ST_MakeLine(points.geom::geometry) AS geom
          FROM ways w
          INNER JOIN (
              SELECT      w.osm_id  AS way_id,
                          wn.index,
                          n.geom
              FROM        ways w
              INNER JOIN  ways_nodes wn ON wn.way_id  = w.osm_id
              INNER JOIN  nodes      n  ON wn.node_id = n.osm_id
              ORDER BY    wn.index
          ) AS points ON points.way_id = w.osm_id
          GROUP BY way_id
      ) AS w ON w.way_id = rw.way_id
      WHERE relation_id = $relationId
    """.query[MultiLineString]
}
