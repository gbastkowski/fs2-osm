package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import fs2.*

object IndustrialFeature extends OptionalFeature {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    IndustrialFeature[F].apply(xa)

  override val tableDefinitions: List[Table] = List(
    Table("industrials",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(fs2.osm.postgres.MultiPolygon, Wgs84))),
    Table("industrials_nodes",
          Column("industrial_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull())),
  )
}

class IndustrialFeature[F[_]: Async] extends Queries {
  def apply(xa: Transactor[F]) =
    Stream
      .eval(simplePolygons(xa))
      .append(complexPolygons(xa))
      .map { "industrials" -> _ }

  private def simplePolygons[F[_]: Async](xa: Transactor[F]) =
    logAndRun(
      sql"""
        INSERT INTO industrials (osm_id, name, geom,                 tags)
        SELECT                   osm_id, name, ST_MakePolygon(geom), tags
        FROM (
            SELECT  ways.osm_id                                       AS osm_id,
                    ways.name                                         AS name,
                    ST_MakeLine(array_agg(nodes.geom)::geometry[])    AS geom,
                    ways.tags                                         AS tags
            FROM                ways
            CROSS JOIN LATERAL  unnest(ways.nodes)                    AS node_id
            INNER JOIN          nodes                                 ON nodes.osm_id = node_id
            WHERE               ways.tags->>'landuse' = 'industrial'
            GROUP BY            ways.osm_id
        ) AS grouped_nodes
        WHERE                   ST_IsClosed(geom)
      """
    ).transact(xa)

  private def complexPolygons[F[_]: Async](xa: Transactor[F]) =
    ComplexPolygonBuilder
      .findMultiPolygonsByTag[ComplexPolygonBuilder.Record]("landuse", "industrial")(ComplexPolygonBuilder.toRecord)
      .transact(xa)
      .map(insert)
      .evalMap(_.transact(xa))

  private def insert(r: ComplexPolygonBuilder.Record) =
    logAndRun(
      sql"""
        INSERT INTO industrials (osm_id, name, tags, geom)
        VALUES                  (${r.osmId}, ${r.name}, ${r.tags}, ${r.geom})
      """
    )
}
