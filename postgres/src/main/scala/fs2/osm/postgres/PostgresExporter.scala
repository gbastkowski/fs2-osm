package fs2.osm
package postgres

import cats.*
import cats.effect.*
import cats.free.Free
import cats.syntax.all.*
import core.*
import doobie.implicits.*
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import fs2.osm.telemetry.Telemetry
import fs2.Stream

/**
 * Saves OSM entities to a Postgres database.
 */
class PostgresExporter[F[_]: Async](features: List[OptionalFeature], telemetry: Telemetry[F], xa: Transactor[F]) extends Queries {
  import Schema.*

  private val allFeatures = ImporterPropertiesFeature :: OsmLineFeature :: PolygonFeature :: features
  private val allTables   = DefaultSchema.tables.toList ::: allFeatures.flatMap { _.tableDefinitions }

  val initSchema: F[Unit] =
    (sql"""CREATE EXTENSION IF NOT EXISTS postgis""" :: allTables.map { t => Fragment.const(t.create) })
      .map { logAndRun }
      .sequence
      .as(())
      .transact(xa)
      .flatTap { _ => Async[F].delay(logger.info("Schema created")) }

  val dropTables: F[Unit] =
    allTables
      .map { t => Fragment.const(t.drop) }
      .map { logAndRun }
      .sequence
      .as(())
      .transact(xa)
      .flatTap { _ => Async[F].delay(logger.info("Schema created")) }

  def runExport(entities: Stream[F, OsmEntity]): F[Summary] =
    for
      _                <- Async[F].delay(logger.info(s"Starting database export"))
      _                <- dropTables
      _                <- initSchema
      nodeCounter      <- telemetry.counter[Long]("importer", "nodes")
      wayCounter       <- telemetry.counter[Long]("importer", "ways")
      relationCounter  <- telemetry.counter[Long]("importer", "relations")
      nodeImporter      = NodeImporter(n => nodeCounter.add(n, telemetry.attributes), xa)
      wayImporter       = WayImporter(n => wayCounter.add(n, telemetry.attributes), xa)
      relationImporter  = RelationImporter(n => relationCounter.add(n, telemetry.attributes), xa)
      importedSummary  <- entities
                            .broadcastThrough(nodeImporter, wayImporter, relationImporter)
                            .map { Summary().insert(_, _) }
                            .foldMonoid
                            .compile.toList.map { _.combineAll }
      featureSummary   <- allFeatures traverse { runFeature } map { _.combineAll }
    yield importedSummary + featureSummary

  def tableExists(name: String): F[Boolean] =
    findFirstN(sql"""
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = $name
      )
    """, 1)
      .transact(xa).compile.toList.map(_.head)

  private def runFeature(feature: Feature): F[Summary] =
    feature
      .run(xa)
      .debug(x => s"saved $x", logger.info)
      .map { (key, value) => Summary().insert(key, value) }
      .foldMonoid.compile.toList.map(_.head)
}
