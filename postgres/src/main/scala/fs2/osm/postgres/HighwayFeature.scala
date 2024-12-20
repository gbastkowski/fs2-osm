package fs2.osm
package postgres

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import doobie.util.fragment.Fragment
import fs2.Stream

object HighwayFeature extends OptionalFeature with Queries {
  override def run[F[_]: Async](xa: Transactor[F]): Stream[F, (String, Int)] =
    Stream
      .emits(dataGenerator.map { (key, operation) => operation.transact(xa).map { key -> _ } })
      .flatMap(Stream.eval)

  override val tableDefinitions: List[Table] =
    List(
      Table("highways",
            Column("osm_id", BigInt, PrimaryKey),
            Column("name", VarChar),
            Column("kind", VarChar, NotNull("''")),
            Column("footway", VarChar),
            Column("sidewalk", VarChar),
            Column("cycleway", VarChar),
            Column("busway", VarChar),
            Column("bicycle_road", VarChar, NotNull("false")),
            Column("surface", VarChar),
            Column("tags", Jsonb),
            Column("geom", Geography(LineString, Wgs84)),
      ),
      Table("highways_nodes",
            Column("highway_id", BigInt, NotNull()),
            Column("node_id", BigInt, NotNull())),
    )

  private val dataGenerator: List[(String, ConnectionIO[Int])] = List(
    "highways" -> logAndRun(sql"""
      INSERT INTO highways (osm_id, name, kind, footway, sidewalk, cycleway, busway, bicycle_road, surface, geom, tags)
      SELECT
          osm_id AS osm_id,
          name   AS name,
          tags->>'highway' AS kind,
          tags->>'footway' AS footway,
          tags->>'sidewalk' AS sidewalk,
          tags->>'cycleway' AS cycleway,
          tags->>'busway' AS busway,
          tags->>'bicycle_road' IS NOT NULL AND tags->>'bicycle_road' = 'yes' AS bicycle_road,
          tags->>'surface' AS surface,
          geom AS geom,
          tags AS tags
      FROM osm_lines
      WHERE tags->>'highway' IS NOT NULL
    """)
  )
}
