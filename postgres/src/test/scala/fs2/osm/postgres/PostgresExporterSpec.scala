package fs2.osm
package postgres

import cats.*
import cats.effect.*
import cats.syntax.all.*
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import doobie.util.transactor.Transactor
import fs2.*
import fs2.osm.core.*
import org.scalacheck.Gen
import org.testcontainers.utility.DockerImageName
import weaver.*
import weaver.scalacheck.*

object PostgresExporterSpec extends IOSuite with Checkers {
  override type Res = PostgresExporter[IO]
  override def sharedResource: Resource[IO, Res] = {
    val acquire = IO {
      EmbeddedPostgres
        .builder()
        .setImage(DockerImageName.parse("postgis/postgis"))
        .start()
        .getPostgresDatabase()
        .getConnection()
    }
    for conn <- Resource.make(acquire) { c => IO(c.close())}
    yield new PostgresExporter[IO](Transactor.fromConnection(conn, logHandler = Option.empty))
  }

  test("PostgresExporter.Summary combines according to Monoid laws") {
    import PostgresExporter.*
    given Show[Summary] = _.toString

    val gen = for
      nodes        <- Gen.posNum[Int]
      ways         <- Gen.posNum[Int]
      updatedWays  <- Gen.posNum[Int]
      relations    <- Gen.posNum[Int]
    yield Summary(nodes, ways, updatedWays, relations)

    forall(Gen.listOf(gen)) { summaries =>
      expect.all(
        summaries.combineAll.nodes       == summaries.map(_.nodes).combineAll,
        summaries.combineAll.ways        == summaries.map(_.ways).combineAll,
        summaries.combineAll.updatedWays == summaries.map(_.updatedWays).combineAll,
        summaries.combineAll.relations   == summaries.map(_.relations).combineAll
      )
    }
  }

  test("PostgresExporter.Summary has an empty according to Monoid laws") {
    import PostgresExporter.*
    given Show[Summary] = _.toString

    val empty = Monoid[Summary].empty
    val gen = for
      nodes        <- Gen.posNum[Int]
      ways         <- Gen.posNum[Int]
      updatedWays  <- Gen.posNum[Int]
      relations    <- Gen.posNum[Int]
    yield Summary(nodes, ways, updatedWays, relations)

    forall(gen) { summary =>
      expect.all(
        Monoid[Summary].combine(summary, empty) == summary,
        Monoid[Summary].combine(empty, summary) == summary,
      )
    }
  }

  test("insert entities") { exporter =>
    val entities = Stream(
      Node(
        osmId = 1,
        coordinate = Coordinate(13.3290697, 52.4519232),
        tags = Map("test" -> "value"),
        info = Info.empty
      ),
      Node(
        osmId = 2,
        coordinate = Coordinate(13.3290697, 52.4519232),
        tags = Map.empty,
        info = Info.empty
      ),
      Way(
        osmId = 3,
        nodes = Seq(1, 2),
        tags = Map("test" -> "value")
      ),
      Relation(
        osmId = 4,
        relations = Seq(
          Relation.Member.Node(
            osmId = 1,
            role = "begin"),
          Relation.Member.Node(
            osmId = 2,
            role = "end"),
          Relation.Member.Way(
            osmId = 3,
            role = "test")
        ),
        tags = Map("testkey" -> "testvalue"),
        info = Info.empty
      ),
      Relation(
        osmId = 5,
        relations = Seq(
          Relation.Member.Relation(
            osmId = 4,
            role = "test")
        ),
        tags = Map("testkey" -> "testvalue"),
        info = Info.empty
      )
    )

    for
      either <- exporter.run(entities).attempt
      summary = either.toTry.get
    yield expect(summary.relations > 0)
  }

  private lazy val nodes = Gen.listOfN(10, node)
  private lazy val node = Gen.oneOf(
    Node(
      osmId = 1,
      coordinate = Coordinate(13.3290697, 52.4519232),
      tags = Map("test" -> "value"),
      info = Info.empty
    ),
    Node(
      osmId = 2,
      coordinate = Coordinate(13.3290697, 52.4519232),
      tags = Map.empty,
      info = Info.empty
    )
  )

  private lazy val relations = Gen.listOfN(2, relation)
  private lazy val relation = Gen.oneOf(
    Relation(
      osmId = 4,
      relations = Seq(
        Relation.Member.Node(osmId = 1, role = "begin"),
        Relation.Member.Node(osmId = 2, role = "end"),
        Relation.Member.Way(osmId = 3, role = "test")),
      tags = Map("testkey" -> "testvalue"),
      info = Info.empty),
    Relation(
      osmId = 5,
      relations = Seq(
        Relation.Member.Relation(osmId = 4, role = "test")),
      tags = Map("testkey" -> "testvalue"),
      info = Info.empty)
  )
}
