package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import fs2.*

object AmenityFeature extends OptionalFeature {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    AmenityFeature[F].apply(xa)

  override val tableDefinitions: List[Table] = List(
    Table("amenities",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(fs2.osm.postgres.MultiPolygon, Wgs84))),
    Table("amenities_nodes",
          Column("amenity_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull())),
  )
}

class AmenityFeature[F[_]: Async] extends Queries {
  def apply(xa: Transactor[F]) =
    Stream
      .eval(simplePolygons(xa))
      .append(complexPolygons(xa))
      .map { "amenities" -> _ }

  private def simplePolygons[F[_]: Async](xa: Transactor[F]) =
    val groupedNodes = sql"""
        SELECT  ways.osm_id                                       AS osm_id,
                ways.name                                         AS name,
                ST_MakeLine(array_agg(nodes.geom)::geometry[])    AS geom,
                ways.tags                                         AS tags
        FROM                ways
        CROSS JOIN LATERAL  unnest(ways.nodes)                    AS node_id
        INNER JOIN          nodes                                 ON nodes.osm_id = node_id
        WHERE               ways.tags->>'landuse' = 'industrial'
        AND                 ways.tags->>'amenity' = 'recycling'
        GROUP BY            ways.osm_id
    """

    val insert = sql"""
        INSERT INTO amenities (osm_id, name, geom,                 tags)
        SELECT                 osm_id, name, ST_MakePolygon(geom), tags
        FROM                  ($groupedNodes)
        WHERE                 ST_IsClosed(geom)
    """

    logAndRun(insert).transact(xa)

  private def complexPolygons[F[_]: Async](xa: Transactor[F]) =
    ComplexPolygonBuilder
      .findMultiPolygonsByTags[ComplexPolygonBuilder.Record]("landuse" -> "industrial", "amenity" -> "recycling")(ComplexPolygonBuilder.toRecord)
      .transact(xa)
      .map(insert)
      .evalMap(_.transact(xa))

  private def insert(r: ComplexPolygonBuilder.Record) =
    logAndRun(
      sql"""
        INSERT INTO amenities (osm_id, name, tags, geom)
        VALUES                (${r.osmId}, ${r.name}, ${r.tags}, ${r.geom})
      """
    )

  // private val tagsFragment =
}
