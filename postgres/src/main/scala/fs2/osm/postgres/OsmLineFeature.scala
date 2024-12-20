package fs2.osm.postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import fs2.Stream

/**
 * Creates PSQL LineStrings from OSM ways
 */
object OsmLineFeature extends Feature with Queries {
  def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    val dataGenerator = List("osm lines" -> logAndRun(getClass.getResource("/insert-into-osm-lines.sql")))
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] = List(
    Table("osm_lines",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(LineString, Wgs84))))
}
