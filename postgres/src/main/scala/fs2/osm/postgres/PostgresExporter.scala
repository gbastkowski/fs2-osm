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
import fs2.{Chunk, Stream}
import io.circe.*
import io.circe.syntax.*
import org.apache.logging.log4j.scala.Logging
import org.postgis.Point
import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import doobie.util.fragment.Fragment

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
      BuildingFeature(),
      RailwayFeature(),
      ProtectedAreaFeature()
    )

  def run(entities: Stream[F, OsmEntity]): F[Summary] = {
    val rawImport = entities
      .map      { handle(Summary()) }
      .chunkN   (100000,                               allowFewer = true)
      .debug    (n => s"inserting ${n.size} entities", logger.debug(_) )
      .flatMap  { chunk => Stream.eval(chunk.combineAll.transact(xa)) }
      .foldMonoid
      .compile
      .toList
      .map      { _.head }

    for
      _        <- createSchema
      summary  <- rawImport
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

  private def insertOsmObject(summaryKey: String, sql: Fragment, summary: Summary) =
    updateRun(sql).transact(xa).map { summary.insert(summaryKey) }

  private def handle(s: Summary)(e: OsmEntity): Free[ConnectionOp, Summary] = e match {
    case n: Node               => handleNode(n).map     { s.insert("nodes")      }
    case r: Relation           => handleRelation(r).map { s.insert("relations")  }
    case w: Way                => handleWay(w).map      { s.insert("ways")       }
  }

  private def handleNode(n: Node) = {
    val osmId = n.osmId
    val geom  = Point(n.coordinate.longitude, n.coordinate.latitude)
    val tags  = toJson(n.tags)
    val name  = n.tags.get("name")
    sql"""
      INSERT INTO nodes (osm_id, name, geom, tags)
      VALUES            ($osmId, $name, $geom, $tags)
    """.update.run
  }

  // private def handleNodes(n: Chunk[Node]) = {
  //   val sql = sql"""INSERT INTO nodes (osm_id, tags) VALUES (?, ?)"""
  //   val tuples = n.map { n =>
  //     (
  //       n.osmId,
  //       Point(n.coordinate.longitude, n.coordinate.latitude),
  //       toJson(n.tags)
  //     )
  //   }
  //   Update[(Long, Point, Json)](sql.update.sql).updateMany(tuples)
  // }


  private def handleWay(w: Way): Free[ConnectionOp, Int] = {
    val osmId     = w.osmId
    val nodes     = w.nodes.toArray
    val tags      = toJson(w.tags)
    val name      = w.tags.get("name")

    val way       = sql"""INSERT INTO ways       (osm_id, name, nodes, tags)    VALUES ($osmId, $name, $nodes, $tags)""".update
    val relations = sql"""INSERT INTO ways_nodes (way_id, node_id)              VALUES (?, ?)""".update

    Seq(
      way.run,
      Update[(Long, Long)](relations.sql).updateMany(nodes.toSeq.map { osmId -> _ })
    ).head
  }

  private def toJson(tags: Map[String, String]) = Json.obj(tags.mapValues { _.asJson }.toSeq: _*)

  private def handleRelation(relation: Relation): Free[ConnectionOp, Int] = {
    val osmId     = relation.osmId
    val name      = relation.tags.get("name")
    val tags      = toJson(relation.tags)
    val nodes     = relation.relations.collect { case n: Relation.Member.Node     => (osmId, n.osmId, n.role) }
    val ways      = relation.relations.collect { case w: Relation.Member.Way      => (osmId, w.osmId, w.role) }
    val relations = relation.relations.collect { case w: Relation.Member.Relation => (osmId, w.osmId, w.role) }

    val insertIntoRelation =
      sql"""
        INSERT INTO relations (osm_id, name, tags)
        VALUES                ($osmId, $name, $tags)
      """.update
    val insertIntoRelationNodes =
      sql"""
        INSERT INTO relations_nodes (relation_id, node_id, role)
        VALUES                      (?, ?, ?)
        ON CONFLICT                 (relation_id, node_id, role) DO NOTHING
      """.update
    val insertIntoRelationWays =
      sql"""
        INSERT INTO relations_ways (relation_id, way_id, role)
        VALUES                     (?, ?, ?)
        ON CONFLICT                (relation_id, way_id, role) DO NOTHING
      """.update
    val insertIntoRelationRelations =
      sql"""
        INSERT INTO relations_relations (parent_id, child_id, role)
        VALUES                          (?, ?, ?)
        ON CONFLICT                     (parent_id, child_id, role) DO NOTHING
      """.update

    Seq(
      insertIntoRelation.run,
      Update[(Long, Long, String)](insertIntoRelationNodes.sql)    .updateMany(nodes),
      Update[(Long, Long, String)](insertIntoRelationWays.sql)     .updateMany(ways),
      Update[(Long, Long, String)](insertIntoRelationRelations.sql).updateMany(relations)
    ).head
  }

  // private def insertInto[T](sql: Fragment, xs: Chunk[T]) =
  //   Update(sql.update.sql)
  //     .updateMany[Chunk](xs.map(Tuple.fromProductTyped))

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
