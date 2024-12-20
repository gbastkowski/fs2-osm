package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import fs2.Stream

object RailwayFeature extends OptionalFeature with Queries {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] = List(
    Table("railways",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("official_name", VarChar),
          Column("operator", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(Polygon, Wgs84))),
    Table("railways_nodes",
          Column("railway_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull()))
  )

  private def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "railways" -> logAndRun(getClass.getResource("/insert-into-railways.sql"))
  )
}
