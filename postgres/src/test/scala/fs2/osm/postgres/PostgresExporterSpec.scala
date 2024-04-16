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
      nodesInserted      <- Gen.posNum[Int]
      nodesUpdated       <- Gen.posNum[Int]
      nodesDeleted       <- Gen.posNum[Int]
      waysInserted       <- Gen.posNum[Int]
      waysUpdated        <- Gen.posNum[Int]
      waysDeleted        <- Gen.posNum[Int]
      relationsInserted  <- Gen.posNum[Int]
      relationsUpdated   <- Gen.posNum[Int]
      relationsDeleted   <- Gen.posNum[Int]
    yield
      Summary(
        SummaryItem(nodesInserted,     nodesUpdated,     nodesDeleted),
        SummaryItem(waysInserted,      waysUpdated,      waysDeleted),
        SummaryItem(relationsInserted, relationsUpdated, relationsDeleted)
      )

    forall(Gen.listOf(gen)) { summaries =>
      expect.all(
        summaries.combineAll.nodes.inserted      == summaries.map(_.nodes.inserted).combineAll,
        summaries.combineAll.nodes.updated       == summaries.map(_.nodes.updated).combineAll,
        summaries.combineAll.nodes.deleted       == summaries.map(_.nodes.deleted).combineAll,
        summaries.combineAll.ways.inserted       == summaries.map(_.ways.inserted).combineAll,
        summaries.combineAll.ways.updated        == summaries.map(_.ways.updated).combineAll,
        summaries.combineAll.ways.deleted        == summaries.map(_.ways.deleted).combineAll,
        summaries.combineAll.relations.inserted  == summaries.map(_.relations.inserted).combineAll,
        summaries.combineAll.relations.updated   == summaries.map(_.relations.updated).combineAll,
        summaries.combineAll.relations.deleted   == summaries.map(_.relations.deleted).combineAll
      )
    }
  }

  test("PostgresExporter.Summary has an empty according to Monoid laws") {
    import PostgresExporter.*
    given Show[Summary] = _.toString

    val empty = Monoid[Summary].empty
    val gen = for
      nodesInserted      <- Gen.posNum[Int]
      nodesUpdated       <- Gen.posNum[Int]
      nodesDeleted       <- Gen.posNum[Int]
      waysInserted       <- Gen.posNum[Int]
      waysUpdated        <- Gen.posNum[Int]
      waysDeleted        <- Gen.posNum[Int]
      relationsInserted  <- Gen.posNum[Int]
      relationsUpdated   <- Gen.posNum[Int]
      relationsDeleted   <- Gen.posNum[Int]
    yield
      Summary(
        SummaryItem(nodesInserted,     nodesUpdated,     nodesDeleted),
        SummaryItem(waysInserted,      waysUpdated,      waysDeleted),
        SummaryItem(relationsInserted, relationsUpdated, relationsDeleted)
      )

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
          Relation.Member.Node(osmId = 1, role = "begin"),
          Relation.Member.Node(osmId = 2, role = "end"),
          Relation.Member.Way(osmId = 3, role = "test")
        ),
        tags = Map("testkey" -> "testvalue"),
        info = Info.empty
      ),
      Relation(
        osmId = 5,
        relations = Seq(
          Relation.Member.Relation(osmId = 4, role = "test")
        ),
        tags = Map("testkey" -> "testvalue"),
        info = Info.empty
      )
    )

    given Eq[PostgresExporter.Summary] = _ == _
    for
      either <- exporter.run(entities).attempt
      summary = either.toTry.get
    yield expect.eql(
      PostgresExporter.Summary(
        nodes     = PostgresExporter.SummaryItem(2, 0, 0),
        ways      = PostgresExporter.SummaryItem(1, 1, 0),
        relations = PostgresExporter.SummaryItem(2, 0, 0)
      ),
      summary
    )
  }

  private lazy val nodes = Gen.listOfN(10, node)
  private lazy val node = Gen.oneOf(
    Node(
      osmId      = 1,
      coordinate = Coordinate(13.3290697, 52.4519232),
      tags       = Map("test" -> "value"),
      info       = Info.empty
    ),
    Node(
      osmId      = 2,
      coordinate = Coordinate(13.3290697, 52.4519232),
      tags       = Map.empty,
      info       = Info.empty
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
