package fs2.osm.postgres

object OsmLineFeature extends Feature {
  override val tableDefinitions: List[Table] = List(
    Table("osm_lines",
          Column("osm_id", BigInt, PrimaryKey),
          Column("name", VarChar),
          Column("tags", Jsonb),
          Column("geom", Geography(LineString, Wgs84)))
    )

  override def dataGenerator = List("osm lines" -> logAndRun(getClass.getResource("/insert-into-osm-lines.sql")))
}
