package fs2.osm.postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import scala.io.Source
import fs2.Stream

object ImporterPropertiesFeature extends Feature {
  def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] =
    List(
      Table("importer_properties",
            Column("key", VarChar, PrimaryKey),
            Column("value", VarChar, NotNull()))
    )

  private def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "importer_properties" -> logAndRun(sql"INSERT INTO importer_properties (key, value) VALUES ('asdf', 'TODO')")
  )

  private def logAndRun(sql: Fragment): ConnectionIO[Int] = {
    logger.debug(sql.update.sql)
    sql.update.run
  }
}
