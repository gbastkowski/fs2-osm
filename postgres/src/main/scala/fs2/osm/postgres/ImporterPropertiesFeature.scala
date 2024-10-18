package fs2.osm.postgres

import doobie.free.ConnectionIO
import doobie.implicits.*
import scala.io.Source

object ImporterPropertiesFeature extends SqlFeature {
  override val tableDefinitions: List[Table] =
    List(
      Table("importer_properties",
            Column("key",         VarChar,                        PrimaryKey),
            Column("value",       VarChar,                        NotNull()))
    )

  override def dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "importer_properties" -> logAndRun(sql"INSERT INTO importer_properties (key, value) VALUES ('asdf', 'TODO')")
  )
}
