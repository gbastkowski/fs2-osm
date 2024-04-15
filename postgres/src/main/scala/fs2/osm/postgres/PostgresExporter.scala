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
      .map { config =>
        Transactor.fromDriverManager[F](
          driver      = "org.postgresql.Driver",
          url         = config.jdbcUrl,
          user        = config.username,
          password    = config.password,
          logHandler  = None
        )
      }
      .map { new PostgresExporter[F](_) }

  case class Config(jdbcUrl: String, username: String, password: String) derives ConfigReader

  case class Summary(
    nodes: Int = 0,
    ways: Int = 0,
    updatedWays: Int = 0,
    relations: Int = 0
  ) {
    def addNodes(n: Int)      = copy(nodes      = nodes     + n)
    def addRelations(n: Int)  = copy(relations  = relations + n)
    def addWays(n: Int)       = copy(ways       = ways      + n)
  }
  case class SummaryItem(inserted: Int = 0, updated: Int = 0)

  given Monoid[Summary] with
    def empty: Summary = Summary()
    def combine(x: Summary, y: Summary): Summary =
      Summary(
        x.nodes           + y.nodes,
        x.ways            + y.ways,
        x.updatedWays     + y.updatedWays,
        x.relations       + y.relations
      )
}

class PostgresExporter[F[_]: Async](xa: Transactor[F]) extends Logging {
  import PostgresExporter.*

  def run(entities: Stream[F, OsmEntity]): F[PostgresExporter.Summary] = {
    val rawImport = entities
      .map      { handle(Summary()) }
      .chunkN(10000, allowFewer = true)
      .flatMap  { chunk => Stream.eval(chunk.combineAll.transact(xa)) }
      .debug(n => s"inserted $n entities", logger.debug(_) )
      .foldMonoid
      .compile
      .toList
      .map      { _.head }

    for
      _        <- createSchema
      summary  <- rawImport
      updated  <- updateWays
    yield summary.copy(updatedWays = updated)
  }

  private def handle(s: Summary)(e: OsmEntity): Free[ConnectionOp, Summary] = e match {
    case n: Node     => handleNode(n).map     { s.addNodes      }
    case r: Relation => handleRelation(r).map { s.addRelations  }
    case w: Way      => handleWay(w).map      { s.addWays       }
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

  private def handleNodes(n: Chunk[Node]) = {
    val sql = sql"""INSERT INTO nodes (osm_id, tags) values (?, ?)"""
    val tuples = n.map { n =>
      (
        n.osmId,
        Point(n.coordinate.longitude, n.coordinate.latitude),
        toJson(n.tags)
      )
    }
    Update[(Long, Point, Json)](sql.update.sql).updateMany(tuples)
  }


  private def handleWay(w: Way): Free[ConnectionOp, Int] = {
    val osmId = w.osmId
    val nodes = w.nodes.toArray
    val tags  = toJson(w.tags)
    val name  = w.tags.get("name")
    val way       = sql"""INSERT INTO ways       (osm_id, name, nodes, tags)    VALUES ($osmId, $name, $nodes, $tags)""".update
    val relations = sql"""INSERT INTO ways_nodes (way_id, node_id)              VALUES (?, ?)""".update

    Seq(
      way.run,
      Update[(Long, Long)](relations.sql).updateMany(nodes.toSeq.map { osmId -> _ })
    ).combineAll
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
        VALUES ($osmId, $name, $tags)
      """.update
    val insertIntoRelationNodes =
      sql"""
        INSERT INTO relations_nodes (relation_id, node_id, role)
        VALUES (?, ?, ?)
        ON CONFLICT (relation_id, node_id, role) DO NOTHING
      """.update
    val insertIntoRelationWays =
      sql"""
        INSERT INTO relations_ways (relation_id, way_id, role)
        VALUES (?, ?, ?)
        ON CONFLICT (relation_id, way_id, role) DO NOTHING
      """.update
    val insertIntoRelationRelations =
      sql"""
        INSERT INTO relations_relations (parent_id, child_id, role)
        VALUES (?, ?, ?)
        ON CONFLICT (parent_id, child_id, role) DO NOTHING
      """.update

    Seq(
      insertIntoRelation.run,
      Update[(Long, Long, String)](insertIntoRelationNodes.sql)    .updateMany(nodes),
      Update[(Long, Long, String)](insertIntoRelationWays.sql)     .updateMany(ways),
      Update[(Long, Long, String)](insertIntoRelationRelations.sql).updateMany(relations)
    ).combineAll
  }

  private val updateWays =
    sql"""
      UPDATE ways
      SET geom = subquery.geom
      FROM (
          SELECT id, ST_MakeLine(points::geometry[]) as geom
          FROM (
              SELECT ways.osm_id AS id, array_agg(nodes.geom) AS points
              FROM ways
              CROSS JOIN LATERAL unnest(ways.nodes) AS node_id
              INNER JOIN nodes ON nodes.osm_id = node_id
              GROUP BY ways.osm_id
          ) AS grouped_nodes
      ) AS subquery
      WHERE osm_id = subquery.id
    """.update.run.transact(xa)

  private val insertIntoLines =
    sql"""
      INSERT INTO lines (osm_id, name, tags, geom) VALUES (
          SELECT osm_id, name, tags, geom FROM ways WHERE ST_IsClosed(geom)
      )
    """.update.run.transact(xa)

  // private def insertInto[T](sql: Fragment, xs: Chunk[T]) =
  //   Update(sql.update.sql)
  //     .updateMany[Chunk](xs.map(Tuple.fromProductTyped))

  private val createSchema =
    List(
      sql"""CREATE EXTENSION IF NOT EXISTS postgis""",

      sql"""DROP TABLE IF EXISTS importer_properties CASCADE""",

      sql"""DROP TABLE IF EXISTS nodes               CASCADE""",

      sql"""DROP TABLE IF EXISTS ways                CASCADE""",
      sql"""DROP TABLE IF EXISTS ways_nodes          CASCADE""",

      sql"""DROP TABLE IF EXISTS relations           CASCADE""",
      sql"""DROP TABLE IF EXISTS relations_nodes     CASCADE""",
      sql"""DROP TABLE IF EXISTS relations_ways      CASCADE""",
      sql"""DROP TABLE IF EXISTS relations_relations CASCADE""",

      sql"""DROP TABLE IF EXISTS lines               CASCADE""",
      sql"""DROP TABLE IF EXISTS polygons            CASCADE""",

      sql"""CREATE TABLE IF NOT EXISTS importer_properties (
              key         varchar                 PRIMARY KEY,
              value       varchar                 NOT NULL
            )""",

      sql"""CREATE TABLE IF NOT EXISTS nodes (
              osm_id      bigint                  PRIMARY KEY,
              name        varchar,
              tags        jsonb                   NOT NULL,
              geom        geography(Point, 4326)  NOT NULL
            )""",

      sql"""CREATE TABLE IF NOT EXISTS ways (
              osm_id      bigint                  PRIMARY KEY,
              name        varchar,
              nodes       bigint[],
              tags        jsonb                   NOT NULL,
              geom        geography(LineString, 4326)
            )""",
      sql"""CREATE TABLE IF NOT EXISTS ways_nodes (
              way_id      bigint                  NOT NULL,
              node_id     bigint                  NOT NULL,

              FOREIGN KEY (way_id)                REFERENCES ways(osm_id),
              FOREIGN KEY (node_id)               REFERENCES nodes(osm_id)
            )""",

      sql"""CREATE TABLE IF NOT EXISTS relations (
              osm_id      bigint                  PRIMARY KEY,
              name        varchar,
              tags        jsonb                   NOT NULL
            )""",
      sql"""CREATE TABLE IF NOT EXISTS relations_nodes (
              relation_id bigint                  NOT NULL,
              node_id     bigint                  NOT NULL,
              role        varchar                 NOT NULL,

              UNIQUE(relation_id, node_id, role)

              -- FOREIGN KEY (relation_id)           REFERENCES relations(osm_id),
              -- FOREIGN KEY (node_id)               REFERENCES nodes(osm_id)
            )""",
      sql"""CREATE TABLE IF NOT EXISTS relations_ways (
              relation_id bigint                  NOT NULL,
              way_id      bigint                  NOT NULL,
              role        varchar                 NOT NULL,

              UNIQUE      (relation_id, way_id, role)

              -- FOREIGN KEY (relation_id)           REFERENCES relations(osm_id),
              -- FOREIGN KEY (way_id)                REFERENCES ways(osm_id)
            )""",
      sql"""CREATE TABLE IF NOT EXISTS relations_relations (
              parent_id   bigint                  NOT NULL,
              child_id    bigint                  NOT NULL,
              role        varchar                 NOT NULL,

              UNIQUE(parent_id, child_id, role)

              -- FOREIGN KEY (parent_id)             REFERENCES relations(osm_id),
              -- FOREIGN KEY (child_id)              REFERENCES relations(osm_id)
            )""",

      sql"""CREATE TABLE IF NOT EXISTS lines (
              osm_id      bigint                  PRIMARY KEY,
              name        varchar,
              tags        jsonb                   NOT NULL,
              geom        geography(LineString, 4326) NOT NULL
            )""",

      sql"""CREATE TABLE IF NOT EXISTS polygons (
              osm_id      bigint                  PRIMARY KEY,
              name        varchar,
              tags        jsonb                   NOT NULL,
              geom        geography(Polygon, 4326) NOT NULL
            )""",
    )
      .map(_.update.run)
      .sequence
      .as(())
      .transact(xa)
}
