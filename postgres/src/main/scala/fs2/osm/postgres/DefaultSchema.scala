package fs2.osm
package postgres

object DefaultSchema extends Schema(
  Table("nodes")
    .withColumns(
      Column("osm_id",      BigInt,                         PrimaryKey),
      Column("name",        VarChar),
      Column("geom",        Geography(Point, Wgs84),        NotNull()),
      Column("tags",        Jsonb,                          NotNull())),

  Table("ways")
    .withColumns(
      Column("osm_id",      BigInt,                         PrimaryKey),
      Column("name",        VarChar),
      Column("nodes",       BigIntArray),
      Column("tags",        Jsonb,                          NotNull()),
      Column("geom",        Geography(LineString, Wgs84))),
  Table("ways_nodes")
    .withColumns(
      Column("way_id",      BigInt,                         NotNull()),
      Column("node_id",     BigInt,                         NotNull()))
    .withConstraints(
      ForeignKeyConstraint("way_id",  "ways"  -> "osm_id"),
      ForeignKeyConstraint("node_id", "nodes" -> "osm_id")),

  Table("relations",
        Column("osm_id",      BigInt,                         PrimaryKey),
        Column("name",        VarChar),
        Column("tags",        Jsonb,                          NotNull())),
  Table("relations_nodes")
    .withColumns(
      Column("relation_id", BigInt,                           NotNull()),
      Column("node_id",     BigInt,                           NotNull()),
      Column("role",        VarChar,                          NotNull()))
    .withConstraints(
      UniqueConstraint("relation_id", "node_id", "role"),
      ForeignKeyConstraint("relation_id",  "relations"  -> "osm_id"),
      ForeignKeyConstraint("node_id",      "nodes"      -> "osm_id")),
  Table("relations_ways")
    .withColumns(
      Column("relation_id", BigInt,                           NotNull()),
      Column("way_id",      BigInt,                           NotNull()),
      Column("role",        VarChar,                          NotNull()))
    .withConstraints(
      UniqueConstraint("relation_id", "way_id", "role")),
  Table("relations_relations")
    .withColumns(
      Column("parent_id",   BigInt),
      Column("child_id",    BigInt),
      Column("role",        VarChar))
    .withConstraints(
      UniqueConstraint("parent_id", "child_id", "role"))
)
