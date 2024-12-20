package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import fs2.*

object LanduseFeature extends OptionalFeature {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    LanduseFeature[F].apply(xa)

  override val tableDefinitions: List[Table] = List(
    Table("landuses",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("kind", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(fs2.osm.postgres.MultiPolygon, Wgs84))),
    Table("landuses_nodes",
          Column("landuse_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull())),
  )
}

class LanduseFeature[F[_]: Async] extends Queries {
  def apply(xa: Transactor[F]) =
    Stream
      .eval(simplePolygons(xa))
      .append(complexPolygons(xa))
      .map { "landuses" -> _ }

  private def simplePolygons[F[_]: Async](xa: Transactor[F]) =
    logAndRun(
      sql"""
        INSERT INTO landuses    (osm_id, name, kind, geom,                 tags)
        SELECT                   osm_id, name, kind, ST_MakePolygon(geom), tags
        FROM (
            SELECT  ways.osm_id                                       AS osm_id,
                    ways.name                                         AS name,
                    ways.tags->>'landuse'                             AS kind,
                    ST_MakeLine(array_agg(nodes.geom)::geometry[])    AS geom,
                    ways.tags                                         AS tags
            FROM                ways
            CROSS JOIN LATERAL  unnest(ways.nodes)                    AS node_id
            INNER JOIN          nodes                                 ON nodes.osm_id = node_id
            WHERE               ways.tags->>'landuse' IS NOT NULL
            GROUP BY            ways.osm_id
        ) AS grouped_nodes
        WHERE                   ST_IsClosed(geom)
      """
    ).transact(xa)

  private def complexPolygons[F[_]: Async](xa: Transactor[F]) =
    ComplexPolygonBuilder
      .findMultiPolygonsWithTag("landuse")(ComplexPolygonBuilder.RecordWithKind.apply)
      .transact(xa)
      .map(insert)
      .evalMap(_.transact(xa))

  private def insert(r: ComplexPolygonBuilder.RecordWithKind) =
    val kind = r.tags.get("landuse")
    logAndRun(
      sql"""
        INSERT INTO landuses (osm_id, name, kind, tags, geom)
        VALUES               (${r.osmId}, ${r.name}, $kind, ${r.tags}, ${r.geom})
      """
    )
}
