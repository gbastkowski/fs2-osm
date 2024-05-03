package fs2.osm
package postgres

import cats.*
import cats.effect.*
import cats.free.Free
import cats.syntax.all.*
import core.*
import doobie.*
import doobie.free.connection.ConnectionOp
import doobie.implicits.*
import doobie.implicits.javatimedrivernative.*
import doobie.postgres.*
import doobie.postgres.circe.json.implicits.*
import doobie.postgres.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import doobie.postgres.pgisimplicits.*
import doobie.util.transactor.Transactor
import fs2.{Chunk, Pipe, Stream}
import io.circe.*
import io.circe.syntax.*
import org.apache.logging.log4j.scala.Logging
import org.postgis.Point
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import doobie.util.fragment.Fragment
import fs2.Pull
import fs2.Stream.ToPull

object PostgresExporter {
  def apply[F[_]: Async]: F[PostgresExporter[F]] =
    ConfigSource
      .default
      .at("db")
      .loadF[F, Config]()
      .map { config => apply(config) }

  def apply[F[_]: Async](config: Config): PostgresExporter[F] =
    new PostgresExporter[F](
      Transactor.fromDriverManager[F](
        driver      = "org.postgresql.Driver",
        url         = config.jdbcUrl,
        user        = config.username,
        password    = config.password,
        logHandler  = None
      )
    )
}

class PostgresExporter[F[_]: Async](xa: Transactor[F]) extends Logging {
  import Schema.*

  private val features =
    List(
      ImporterPropertiesFeature,
      OsmLineFeature,
      HighwayFeature,
      WaterFeature,
      BuildingFeature,
      RailwayFeature,
      ProtectedAreaFeature
    )

  def run(entities: Stream[F, OsmEntity]): F[Summary] =
    for
      _        <- createSchema
      summary1 <- entities
                    .broadcastThrough(
                      NodeImporter(xa),
                      WayImporter(xa),
                      RelationImporter(xa))
                    .map { Summary().insert(_, _) }
                    .foldMonoid
                    .compile.toList.map { _.combineAll }
      summary2 <- runFeatures
    yield summary1 + summary2

  private def runFeatures: F[Summary] = features traverse { runFeature } map { _.combineAll }

  private def runFeature(feature: Feature) =
    feature
      .run(xa)
      .map { _.map { (key, value) => Summary().insert(key, value) } }
      .sequence
      .map { _.combineAll }

  private lazy val createSchema = {
    val allTables    = DefaultSchema.tables.toList ::: features.flatMap { _.tableDefinitions }
    val initSchema   = sql"""CREATE EXTENSION IF NOT EXISTS postgis"""
    val dropTables   = allTables.map { t => Fragment.const(t.drop) }
    val createTables = allTables.map { t => Fragment.const(t.create) }

    (initSchema :: dropTables ::: createTables)
      .map { updateRun }
      .sequence
      .as(())
      .transact(xa)
  }

  private def updateRun(sql: Fragment) = {
    logger.debug(sql.update.sql)
    sql.update.run
  }
}
