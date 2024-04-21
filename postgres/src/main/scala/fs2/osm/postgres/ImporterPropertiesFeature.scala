package fs2.osm.postgres

import doobie.implicits.*
import doobie.util.fragment.Fragment
import scala.io.Source

class ImporterPropertiesFeature extends Feature {
  override val name: String = "importer_properties"

  override val tableDefinitions: List[Table] =
    List(
      Table("importer_properties",
            Column("key",         VarChar,                        PrimaryKey),
            Column("value",       VarChar,                        NotNull()))
    )

  override def dataGenerator: List[Fragment] = List(
    sql"INSERT INTO importer_properties (key, value) VALUES ('asdf', 'TODO')"
  )
}
