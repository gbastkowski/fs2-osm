package fs2.osm.postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import fs2.Stream

object ProtectedAreaFeature extends Feature with Queries {
  def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] =
    List(
      Table("protected_areas",
            Column("osm_id", BigInt, PrimaryKey),
            Column("name", VarChar),
            Column("kind", VarChar),
            Column("tags", Jsonb),
            Column("geom", Geography(Polygon, Wgs84)),
      ),
      Table("protected_areas_nodes",
            Column("protected_area_id", BigInt, NotNull()),
            Column("node_id", BigInt, NotNull())),
    )

  private def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "protected areas" -> logAndRun(getClass.getResource("/insert-into-protected-areas.sql"))
  )
}
