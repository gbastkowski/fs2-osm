package fs2.osm
package postgres

object DefaultSchema extends Schema(
  Table("importer_properties",
        Column("key",         VarChar),
        Column("value",       VarChar)),

  Table("nodes",
        Column("osm_id",      BigInt),
        Column("name",        VarChar),
        Column("geom",        Geography(Point, `4326`)),
        Column("tags",        Jsonb)),

  Table("ways",
        Column("osm_id",      BigInt),
        Column("name",        VarChar),
        Column("tags",        Jsonb)),
  Table("ways_nodes",
        Column("way_id",      BigInt),
        Column("node_id",     BigInt)),

  Table("relations",
        Column("osm_id",      BigInt),
        Column("name",        VarChar),
        Column("tags",        Jsonb)),
  Table("relations_nodes",
        Column("relation_id", BigInt),
        Column("node_id",     BigInt),
        Column("role",        VarChar)),
  Table("relations_ways",
        Column("relation_id", BigInt),
        Column("way_id",      BigInt),
        Column("role",        VarChar)),
  Table("relations_relations",
        Column("parent_id",   BigInt),
        Column("child_id",    BigInt),
        Column("role",        VarChar))
)
