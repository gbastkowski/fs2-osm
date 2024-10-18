package fs2.osm.postgres

import doobie.free.ConnectionIO
import doobie.util.fragment.Fragment
import scala.io.Source

object ProtectedAreaFeature extends SqlFeature {
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

  override def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "protected areas" -> logAndRun(getClass.getResource("/insert-into-protected-areas.sql"))
  )
}
