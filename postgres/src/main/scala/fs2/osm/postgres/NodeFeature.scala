package fs2.osm
package postgres

import doobie.*
import doobie.implicits.*
import doobie.util.fragment.Fragment

class NodeFeature extends Feature {
  override val name: String = "nodes"

  override val tableDefinitions: List[Table] = List(
    Table("nodes")
      .withColumns(
        Column("osm_id",      BigInt,                         PrimaryKey),
        Column("name",        VarChar),
        Column("geom",        Geography(Point, Wgs84),        NotNull()),
        Column("tags",        Jsonb,                          NotNull()))
  )

  override def dataGenerator: List[Fragment] = List(
    sql"""
      INSERT INTO nodes (osm_id, name, geom, tags)
      VALUES            (?, ?, ?, ?)
    """
  )
}
