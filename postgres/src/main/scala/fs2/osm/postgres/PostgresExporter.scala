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
  def run(entities: Stream[F, OsmEntity]): F[Summary] = {
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
      _        <- writeImporterProperties
      summary  <- rawImport
    yield summary
  }

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

  private lazy val createSchema =
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

      sql"""DROP TABLE IF EXISTS highways            CASCADE""",
      sql"""DROP TABLE IF EXISTS highways_nodes      CASCADE""",
      sql"""DROP TABLE IF EXISTS railways            CASCADE""",
      sql"""DROP TABLE IF EXISTS railways_nodes      CASCADE""",
      sql"""DROP TABLE IF EXISTS buildings           CASCADE""",
      sql"""DROP TABLE IF EXISTS buildings_nodes     CASCADE""",
      sql"""DROP TABLE IF EXISTS protected_areas     CASCADE""",
      sql"""DROP TABLE IF EXISTS protected_areas_nodes     CASCADE""",

      importer_properties,
      nodes,
      ways, ways_nodes,
      relations, relations_nodes, relations_ways, relations_relations,
      highways, highways_nodes,
      railways, railways_nodes,
      buildings, buildings_nodes,
      protectedAreas, protectedAreas_nodes,
    )
      .map(_.update.run)
      .sequence
      .as(())
      .transact(xa)

  extension (t: Table)
    def create = s"""
      CREATE TABLE IF NOT EXISTS ${t.name} (
        osm_id      bigint                    PRIMARY KEY,
        name        varchar,
        nodes       bigint[],
        tags        jsonb                     NOT NULL,
        geom        geography(LineString, 4326)
      )
    """

    def drop = s"""
      DROP TABLE IF EXISTS ${t.name}          CASCADE
    """

  private lazy val writeImporterProperties =
    Update[(String, String)](sql"INSERT INTO importer_properties (key, value) VALUES (?, ?)".update.sql)
      .updateMany(Seq("source" -> "TODO"))
      .transact(xa)

  private lazy val importer_properties = sql"""
    CREATE TABLE IF NOT EXISTS importer_properties (
      key         varchar                   PRIMARY KEY,
      value       varchar                   NOT NULL
    )
  """

  private lazy val nodes = sql"""
    CREATE TABLE IF NOT EXISTS nodes (
      osm_id      bigint                    PRIMARY KEY,
      name        varchar,
      tags        jsonb                     NOT NULL,
      geom        geography(Point, 4326)    NOT NULL
    )
  """

  private lazy val ways = sql"""
    CREATE TABLE IF NOT EXISTS ways (
      osm_id      bigint                    PRIMARY KEY,
      name        varchar,
      nodes       bigint[],
      tags        jsonb                     NOT NULL,
      geom        geography(LineString, 4326)
    )
  """

  private lazy val highways = sql"""
    CREATE TABLE IF NOT EXISTS highways (
      osm_id        bigint                    PRIMARY KEY,
      name          varchar,
      kind          varchar                   NOT NULL DEFAULT '',
      footway       varchar,
      sidewalk      varchar,
      cycleway      varchar,
      busway        varchar,
      bicycle_road  boolean                   NOT NULL DEFAULT false,
      surface       varchar,
      nodes         bigint[],
      tags          jsonb                     NOT NULL,
      geom          geography(LineString, 4326)
    )
  """

  private lazy val highways_nodes = sql"""
    CREATE TABLE IF NOT EXISTS highways_nodes (
      highway_id  bigint                    NOT NULL,
      node_id     bigint                    NOT NULL,

      FOREIGN KEY (highway_id)              REFERENCES highways(osm_id),
      FOREIGN KEY (node_id)                 REFERENCES nodes(osm_id)
    )
  """

  private lazy val railways = sql"""
    CREATE TABLE IF NOT EXISTS railways (
      osm_id        bigint                    PRIMARY KEY,
      name          varchar,
      official_name varchar,
      operator      varchar,
      nodes         bigint[],
      tags          jsonb                     NOT NULL,
      geom          geography(Polygon, 4326)
    )
  """

  private lazy val railways_nodes = sql"""
    CREATE TABLE IF NOT EXISTS railways_nodes (
      railway_id  bigint                    NOT NULL,
      node_id     bigint                    NOT NULL,

      FOREIGN KEY (railway_id)              REFERENCES railways(osm_id),
      FOREIGN KEY (node_id)                 REFERENCES nodes(osm_id)
    )
  """

  private lazy val buildings = sql"""
    CREATE TABLE IF NOT EXISTS buildings (
      osm_id        bigint                    PRIMARY KEY,
      name          varchar,
      kind          varchar,
      nodes         bigint[],
      tags          jsonb                     NOT NULL,
      geom          geography(Polygon, 4326)
    )
  """

  private lazy val buildings_nodes = sql"""
    CREATE TABLE IF NOT EXISTS buildings_nodes (
      building_id  bigint                   NOT NULL,
      node_id     bigint                    NOT NULL,

      FOREIGN KEY (building_id)             REFERENCES buildings(osm_id),
      FOREIGN KEY (node_id)                 REFERENCES nodes(osm_id)
    )
  """

  private lazy val protectedAreas = sql"""
    CREATE TABLE IF NOT EXISTS protected_areas (
      osm_id        bigint                    PRIMARY KEY,
      name          varchar,
      kind          varchar,
      nodes         bigint[],
      tags          jsonb                     NOT NULL,
      geom          geography(Polygon, 4326)
    )
  """

  private lazy val protectedAreas_nodes = sql"""
    CREATE TABLE IF NOT EXISTS protected_areas_nodes (
      protected_area_id  bigint             NOT NULL,
      node_id     bigint                    NOT NULL,

      FOREIGN KEY (protected_area_id)       REFERENCES protected_areas(osm_id),
      FOREIGN KEY (node_id)                 REFERENCES nodes(osm_id)
    )
  """

  private lazy val ways_nodes = sql"""
    CREATE TABLE IF NOT EXISTS ways_nodes (
      way_id      bigint                    NOT NULL,
      node_id     bigint                    NOT NULL

      -- FOREIGN KEY (way_id)                REFERENCES ways(osm_id),
      -- FOREIGN KEY (node_id)               REFERENCES nodes(osm_id)
    )
  """

  private lazy val relations = sql"""
    CREATE TABLE IF NOT EXISTS relations (
      osm_id      bigint                    PRIMARY KEY,
      name        varchar,
      tags        jsonb                     NOT NULL
    )
  """

  private lazy val relations_nodes = sql"""
    CREATE TABLE IF NOT EXISTS relations_nodes (
      relation_id bigint                    NOT NULL,
      node_id     bigint                    NOT NULL,
      role        varchar                   NOT NULL,

      UNIQUE(relation_id, node_id, role)

      -- FOREIGN KEY (relation_id)           REFERENCES relations(osm_id),
      -- FOREIGN KEY (node_id)               REFERENCES nodes(osm_id)
    )
  """

  private lazy val relations_ways = sql"""
    CREATE TABLE IF NOT EXISTS relations_ways (
      relation_id bigint                    NOT NULL,
      way_id      bigint                    NOT NULL,
      role        varchar                   NOT NULL,

      UNIQUE      (relation_id, way_id, role)

      -- FOREIGN KEY (relation_id)           REFERENCES relations(osm_id),
      -- FOREIGN KEY (way_id)                REFERENCES ways(osm_id)
    )
  """

  private lazy val relations_relations = sql"""
    CREATE TABLE IF NOT EXISTS relations_relations (
      parent_id   bigint                    NOT NULL,
      child_id    bigint                    NOT NULL,
      role        varchar                   NOT NULL,

      UNIQUE(parent_id, child_id, role)

      -- FOREIGN KEY (parent_id)             REFERENCES relations(osm_id),
      -- FOREIGN KEY (child_id)              REFERENCES relations(osm_id)
    )
  """
}
