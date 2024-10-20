package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import fs2.*
import org.apache.logging.log4j.scala.Logging
import doobie.free.connection.ConnectionIO
import doobie.free.connection.ConnectionOp
import cats.free.Free

object WaterFeature2 extends Feature with MultiPolygonBuilder {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    findMultiPolygonsByTag("natural", "water")
      .transact(xa)
      .map(insert)
      .evalMap(_.transact(xa))
      .map { "waters" -> _ }

  override val tableDefinitions: List[Table] = Nil

  private def insert(r: Record) = {
    val kind = r.tags.getOrElse("water", "unspecified")
    logAndRun(
      sql"""
        INSERT INTO waters  (osm_id, name, kind, tags, geom)
        VALUES              (${r.osmId}, ${r.name}, $kind, ${r.tags}, ${r.geom})
      """
    )
  }

  private def logAndRun(sql: Fragment): ConnectionIO[Int] = sql.update.run
}
