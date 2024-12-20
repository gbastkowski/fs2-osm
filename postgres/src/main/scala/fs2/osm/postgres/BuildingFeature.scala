package fs2.osm.postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import fs2.Stream
import java.net.URL
import scala.io.Source

object BuildingFeature extends OptionalFeature with Queries {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    val dataGenerator = List("buildings" -> logAndRun(getClass.getResource("/insert-into-buildings.sql")))
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] = List(
    Table("buildings",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("kind", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(Polygon, Wgs84))),
    Table("buildings_nodes",
          Column("building_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull())),
  )
}
