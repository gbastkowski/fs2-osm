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
import java.{util => ju}

object SummarySpec extends SimpleIOSuite with Checkers {
  test("Summary combines according to Monoid laws") {
    given Show[Summary] = _.toString

    val gen = Gen.listOf(
      for
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
          Map(
            "nodes"        -> SummaryItem(nodesInserted,     nodesUpdated,     nodesDeleted),
            "ways"         -> SummaryItem(waysInserted,      waysUpdated,      waysDeleted),
            "relations"    -> SummaryItem(relationsInserted, relationsUpdated, relationsDeleted)
          )
        )
    )

    forall(genSummaries) { summaries =>
      val subject      = summaries.combineAll
      val actualKeys   = subject.operations.keySet

      val keysExpect = expect.eql(summaries.toSet.flatMap { _.operations.keySet }, actualKeys)
      val insertedExpects =
        actualKeys.toList.map { key =>
          subject.get(key)
          try
            expect.eql(
              subject.get(key).inserted,
              summaries.mapFilter { _.operations.get(key) }.map { _.inserted }.combineAll
            )
          catch case ex: ju.NoSuchElementException => failure(ex.getMessage + "\n" + summaries + "\n" + subject)
        }

      (keysExpect :: insertedExpects).combineAll
    }
  }

  test("Summary has an empty according to Monoid laws") {
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
      polygonsInserted   <- Gen.posNum[Int]
      polygonsUpdated    <- Gen.posNum[Int]
      polygonsDeleted    <- Gen.posNum[Int]
      relationsInserted  <- Gen.posNum[Int]
      relationsUpdated   <- Gen.posNum[Int]
      relationsDeleted   <- Gen.posNum[Int]
    yield
      Summary(
        Map(
          "nodes"        -> SummaryItem(nodesInserted,     nodesUpdated,     nodesDeleted),
          "ways"         -> SummaryItem(waysInserted,      waysUpdated,      waysDeleted),
          "polygons"     -> SummaryItem(polygonsInserted,  polygonsUpdated,  polygonsDeleted),
          "relations"    -> SummaryItem(relationsInserted, relationsUpdated, relationsDeleted)
        )
      )

    forall(gen) { summary =>
      expect.all(
        Monoid[Summary].combine(summary, empty) == summary,
        Monoid[Summary].combine(empty, summary) == summary,
      )
    }
  }

  private lazy val genSummaries = Gen.listOfN(3, genSummary)

  private lazy val genSummary =
    Gen
      .mapOfN(5,
        for
          operationKey <- Gen.oneOf("a", "b", "c", "nodes", "ways")
          inserted     <- Gen.posNum[Int]
          updated      <- Gen.posNum[Int]
          deleted      <- Gen.posNum[Int]
        yield operationKey -> SummaryItem(inserted, updated, deleted)
      )
      .map { Summary(_) }

  private given Show[Summary] = _.toString
}
