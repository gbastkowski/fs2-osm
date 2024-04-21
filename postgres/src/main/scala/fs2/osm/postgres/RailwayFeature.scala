package fs2.osm.postgres

import doobie.util.fragment.Fragment
import scala.io.Source

class RailwayFeature extends Feature {
  override val name: String = "railways"

  override val tableDefinitions: List[Table] = List(
    Table("railways",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("official_name", VarChar),
          Column("operator", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(Polygon, Wgs84))),
    Table("railways_nodes",
          Column("railway_id", BigInt, NotNull()),
          Column("node_id", BigInt, NotNull()))
  )

  override def dataGenerator: List[Fragment] = List(
    Fragment.const(Source.fromURL(getClass.getResource("/insert-into-railways.sql")).mkString)
  )
}
