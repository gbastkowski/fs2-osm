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
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import fs2.Pull
import fs2.Stream.ToPull
import fs2.osm.telemetry.Telemetry
import fs2.{Chunk, Pipe, Stream}
import io.circe.*
import io.circe.syntax.*
import net.postgis.jdbc.geometry.*
import org.apache.logging.log4j.scala.Logging
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Meter
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*

class PostgresExporter[F[_]: Async](features: List[Feature], telemetry: Telemetry[F], xa: Transactor[F]) extends Logging {
  import Schema.*

  def run(entities: Stream[F, OsmEntity]): F[Summary] =
    for
      _                <- Async[F].delay(logger.info(s"Starting database export"))
      _                <- createSchema
      nodeCounter      <- telemetry.counter[Long]("importer", "nodes")
      wayCounter       <- telemetry.counter[Long]("importer", "ways")
      relationCounter  <- telemetry.counter[Long]("importer", "relations")
      nodeImporter      = NodeImporter(n => nodeCounter.add(n, telemetry.attributes), xa)
      wayImporter       = WayImporter(n => wayCounter.add(n, telemetry.attributes), xa)
      relationImporter  = RelationImporter(n => relationCounter.add(n, telemetry.attributes), xa)
      summary1         <- entities
                            .broadcastThrough(nodeImporter, wayImporter, relationImporter)
                            .map { Summary().insert(_, _) }
                            .foldMonoid
                            .compile.toList.map { _.combineAll }
      summary2         <- runFeatures
    yield summary1 + summary2

  private def runFeatures: F[Summary] = features traverse { runFeature } map { _.combineAll }

  private def runFeature(feature: Feature): F[Summary] =
    feature
      .run(xa)
      .map { (key, value) => Summary().insert(key, value) }
      .foldMonoid.compile.toList.map(_.head)

  private lazy val createSchema =
    val allTables    = DefaultSchema.tables.toList ::: features.flatMap { _.tableDefinitions }
    val initSchema   = sql"""CREATE EXTENSION IF NOT EXISTS postgis"""
    val dropTables   = allTables.map { t => Fragment.const(t.drop) }
    val createTables = allTables.map { t => Fragment.const(t.create) }

    (initSchema :: dropTables ::: createTables)
      .map { updateRun }
      .sequence
      .as(())
      .transact(xa)

  private def updateRun(sql: Fragment) =
    logger.debug(sql.update.sql)
    sql.update.run
}
