package fs2.osm.postgres

import doobie.util.fragment.Fragment
import scala.io.Source

class ProtectedAreaFeature extends Feature {
  override val name: String = "protected areas"

  override val tableDefinitions: List[Table] =
    List(
      Table("protected_areas",
            Column("osm_id", BigInt, PrimaryKey),
            Column("name", VarChar),
            Column("kind", VarChar),
            Column("tags", Jsonb),
            Column("geom", Geography(Polygon, Wgs84)),
      ),
      Table("protected_areas_nodes",
            Column("protected_area_id", BigInt, NotNull()),
            Column("node_id", BigInt, NotNull())),
    )

  override def dataGenerator: List[Fragment] = List(
    Fragment.const(Source.fromURL(getClass.getResource("/insert-into-protected-areas.sql")).mkString)
  )
}