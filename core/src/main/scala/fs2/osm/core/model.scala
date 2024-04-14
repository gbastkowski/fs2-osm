package fs2.osm
package core

import java.time.Instant
import org.openstreetmap.osmosis.osmbinary.osmformat

sealed trait OsmEntity {
  val osmId: Long
  val tags: Map[String, String]
  val info: Info

  override def toString(): String = s"${getClass().getSimpleName()}(osmId = $osmId)"
}

object Relation {
  def apply(stringTable: osmformat.StringTable, relation: osmformat.Relation): Relation = {
    val relations =
      (relation.memids.scanLeft(0L) { _ + _ }.drop(1), relation.types, relation.rolesSid)
        .zipped
        .map { (id, types, roles) => Member(types.value, id, stringTable.getString(roles)) }

    Relation(
      relation.id,
      relations,
      stringTable.tags(relation.keys, relation.vals)
    )
  }

  sealed trait Member {
    val osmId: Long
    val role: String
  }

  object Member {
    case class Node         (osmId: Long, role: String) extends Member
    case class Way          (osmId: Long, role: String) extends Member
    case class Relation     (osmId: Long, role: String) extends Member
    case class Unrecognized (osmId: Long, role: String) extends Member

    def apply(value: Int, osmId: Long, role: String) = value match {
      case 0 => Node(osmId, role)
      case 1 => Way(osmId, role)
      case 2 => Relation(osmId, role)
      case _ => Unrecognized(osmId, role)
    }
  }
}

case class Relation(
  osmId: Long,
  relations: Seq[Relation.Member],
  tags: Map[String, String],
  info: Info = Info.empty
) extends OsmEntity

object Way {
  def apply(stringTable: osmformat.StringTable, way: osmformat.Way): Way = {
    Way(
      way.id,
      way.refs.scanLeft(0L) { _ + _ }.drop(1),
      stringTable.tags(way.keys, way.vals),
      Info(stringTable, way.info)
    )
  }
}

case class Way(
  osmId: Long,
  nodes: Seq[Long],
  tags: Map[String, String],
  info: Info = Info.empty
) extends OsmEntity

case class Node(
  osmId: Long,
  coordinate: Coordinate,
  tags: Map[String, String],
  info: Info
) extends OsmEntity

object Info {
  def apply(stringTable: osmformat.StringTable, maybeInfo: Option[osmformat.Info]): Info =
    maybeInfo.fold(empty) { info =>
        Info(
          info.version,
          info.timestamp.map(Instant.ofEpochSecond),
          info.changeset,
          info.uid,
          info.userSid.map(idx => stringTable.getString(idx))
        )
    }

  val empty: Info = Info()
}

case class Info(
  version: Option[Int] = None,
  timestamp: Option[Instant] = None,
  changeset: Option[Long] = None,
  userId: Option[Int] = None,
  userName: Option[String] = None,
  visible: Option[Boolean] = None
) {
  def isEmpty: Boolean = (version ++ timestamp ++ changeset ++ userId ++ userName ++ visible).isEmpty
  def nonEmpty: Boolean = !isEmpty
}
