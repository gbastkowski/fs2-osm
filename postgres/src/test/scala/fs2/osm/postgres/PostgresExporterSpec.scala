package fs2.osm
package postgres

import cats.*
import cats.effect.*
import cats.syntax.all.*
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import doobie.util.transactor.Transactor
import fs2.*
import fs2.osm.core.*
import fs2.osm.telemetry.*
import org.scalacheck.Gen
import org.testcontainers.utility.DockerImageName
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{Counter, MeasurementValue}
import scala.collection.immutable
import weaver.*
import weaver.scalacheck.*

object PostgresExporterSpec extends IOSuite with Checkers:
  override type Res = PostgresExporter[IO]
  override def sharedResource: Resource[IO, Res] =
    val acquire = IO {
      EmbeddedPostgres
        .builder()
        .setImage(DockerImageName.parse("postgis/postgis"))
        .start()
        .getPostgresDatabase()
        .getConnection()
    }
    for
      conn       <- Resource.make(acquire) { c => IO(c.close()) }
      transactor  = Transactor.fromConnection(conn, logHandler = Option.empty)
    yield new PostgresExporter[IO](features = Nil, TelemetryStub, transactor)

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

    for
      either <- exporter.run(entities).attempt
      summary = either.toTry.get
    yield expect.all(
      summary.get("nodes")      == SummaryItem(2, 0, 0),
      summary.get("ways")       == SummaryItem(3, 0, 0),
      summary.get("relations")  == SummaryItem(6, 0, 0)
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
      info = Info.empty))
