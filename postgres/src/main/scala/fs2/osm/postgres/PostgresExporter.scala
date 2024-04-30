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
      ImporterPropertiesFeature(),
      HighwayFeature(),
      WaterFeature(),
      BuildingFeature(),
      RailwayFeature(),
      ProtectedAreaFeature()
    )

  def run(entities: Stream[F, OsmEntity]): F[Summary] = {
    val importEntities = entities
      .broadcastThrough(
        _.collect { case n: Node     => n } .chunkN(10000, allowFewer = true) .map { n => handleNodes(n)     .map { Summary().insert("nodes")     } },
        _.collect { case w: Way      => w } .chunkN(10000, allowFewer = true) .map { w => handleWays(w)      .map { Summary().insert("ways")      } },
        _.collect { case r: Relation => r } .chunkN(10000, allowFewer = true) .map { r => handleRelations(r) .map { Summary().insert("relations") } })
      .evalMap { _.transact(xa) }

    for
      _        <- createSchema
      summary  <- importEntities.foldMonoid.compile.toList.map { _.combineAll }
      summary  <- runFeatures(summary)
    yield summary
  }

  private def runFeatures(summary: Summary): F[Summary] =
    features
      .traverse { feature =>
        feature.dataGenerator
          .map { updateRun }
          .map { _.transact(xa) }
          .sequence
          .map { _.map { summary.insert(feature.name) } }
          .map { _.combineAll }
      }
      .map { _.combineAll }

  private def handleNodes(chunk: Chunk[Node]) = {
    val sql = "INSERT INTO nodes (osm_id, name, geom, tags) VALUES (?, ?, ?, ?)"
    val tuples = chunk.map { n =>
      (
        n.osmId,
        n.tags.get("name"),
        Point(n.coordinate.longitude, n.coordinate.latitude),
        toJson(n.tags)
      )
    }
    Update[(Long, Option[String], Point, Json)](sql).updateMany(tuples)
  }

  private def handleWays(chunk: Chunk[Way]) = {
    val ways = chunk.map { w =>
      (
        w.osmId,
        w.tags.get("name"),
        w.nodes.toArray,
        toJson(w.tags)
      )
    }

    val nodesWithIndex = chunk.toList.flatMap { way => way.nodes.zipWithIndex.map { (nodeId, index) => (way.osmId, nodeId, index) } }

    Seq(
      Update[(Long, Option[String], Array[Long], Json)]("INSERT INTO ways (osm_id, name, nodes, tags) VALUES (?, ?, ?, ?)").updateMany(ways),
      Update[(Long, Long, Int)]("INSERT INTO ways_nodes (way_id, node_id, index) VALUES (?, ?, ?)").updateMany(nodesWithIndex)
    ).combineAll
  }

  private def handleRelations(chunk: Chunk[Relation]): Free[ConnectionOp, Int] = {
    val relations = chunk.toList.map { relation =>
      (
        relation.osmId,
        relation.tags.get("name"),
        relation.tags.get("type"),
        toJson(relation.tags)
      )
    }
    val relationsWithIndex = chunk.toList.flatMap { relation => relation.relations.zipWithIndex.map { (relation.osmId, _, _) } }

    Seq(
      Update[(Long, Option[String], Option[String], Json)](
        """
          INSERT INTO relations           (osm_id, name,  type,          tags)
          VALUES                          (?, ?, ?, ?)
        """
      ).updateMany(relations),
      Update[(Long, Long, Int, String)](
        """
          INSERT INTO relations_nodes     (relation_id, node_id, index, role)
          VALUES                          (?, ?, ?, ?)
          ON CONFLICT                     (relation_id, node_id, role) DO NOTHING
        """
      ).updateMany(relationsWithIndex.collect { case (osmId, n: Relation.Member.Node, index)     => (osmId, n.osmId, index, n.role) }),
      Update[(Long, Long, Int, String)](
        """
          INSERT INTO relations_ways      (relation_id, way_id, index, role)
          VALUES                          (?, ?, ?, ?)
          ON CONFLICT                     (relation_id, way_id, role) DO NOTHING
        """
      ).updateMany(relationsWithIndex.collect { case (osmId, w: Relation.Member.Way, index)      => (osmId, w.osmId, index, w.role) }),
      Update[(Long, Long, Int, String)](
        """
          INSERT INTO relations_relations (parent_id, child_id, index, role)
          VALUES                          (?, ?, ?, ?)
          ON CONFLICT                     (parent_id, child_id, role) DO NOTHING
        """
      ).updateMany(relationsWithIndex.collect { case (osmId, w: Relation.Member.Relation, index) => (osmId, w.osmId, index, w.role) })
    ).combineAll
  }

  private def toJson(tags: Map[String, String]) = Json.obj(tags.mapValues { _.asJson }.toSeq: _*)

  private lazy val createSchema = {
    val allTables    = DefaultSchema.tables.toList ::: features.flatMap { _.tableDefinitions }
    val initSchema   = sql"""CREATE EXTENSION IF NOT EXISTS postgis"""
    val dropTables   = allTables.map { t => Fragment.const(t.drop) }
    val createTables = allTables.map { t=> Fragment.const(t.create) }

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
