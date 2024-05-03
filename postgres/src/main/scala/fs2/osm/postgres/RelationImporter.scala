package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import core.{OsmEntity, Relation}
import doobie.*
import doobie.implicits.*
import doobie.implicits.javatimedrivernative.*
import doobie.postgres.*
import doobie.postgres.circe.json.implicits.*
import doobie.postgres.implicits.*
import doobie.postgres.pgisgeographyimplicits.*
import doobie.postgres.pgisimplicits.*
import fs2.{Chunk, Pipe}
import io.circe.Json

object RelationImporter {
  def apply[F[_]: Async](xa: Transactor[F]): Pipe[F, OsmEntity, (String, Int)] = _
    .collect { case n: Relation => n }
    .chunkN(10000, allowFewer = true)
    .map { handleRelations }
    .evalMap { _.transact(xa) }
    .map { "relations" -> _ }

  private def handleRelations(chunk: Chunk[Relation]) = {
    val relations = chunk.toList.map { relation =>
      (
        relation.osmId,
        relation.tags.get("name"),
        relation.tags.get("type"),
        relation.tags.toJson
      )
    }
    val relationsWithIndex = chunk.toList.flatMap { relation => relation.relations.zipWithIndex.map { (relation.osmId, _, _) } }

    Seq(
      Update[(Long, Option[String], Option[String], Json)](
        """
          INSERT INTO relations           (osm_id, name, type, tags)
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

  def find(maintype: String, subtype: String) =
    sql"""
      SELECT  osm_id, name, tags
      FROM    relations
      WHERE   relations.type = '$maintype'
      AND     relations.tags->>'$maintype' = '$subtype'
    """
      .query[(Long, Option[String], Map[String, String])]
}
