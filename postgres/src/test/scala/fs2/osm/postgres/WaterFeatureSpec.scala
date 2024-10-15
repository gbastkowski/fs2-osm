package fs2.osm
package postgres

import cats.effect.*
import cats.implicits.*
import doobie.*
import doobie.implicits.*
import weaver.*

object WaterFeatureSpec extends IOSuite {
  import WaterFeature.*
  import Schema.*

  override type Res = Transactor[IO]
  override def sharedResource: Resource[IO, Res] = {
    val xa = Config("jdbc:postgresql:fs2-osm", "gunnar", "").transactor

    val acquire =
      for
        _  <- Fragment.const(WaterFeature.tableDefinitions.head.drop).update.run.transact(xa)
        _  <- Fragment.const(WaterFeature.tableDefinitions.head.create).update.run.transact(xa)
      yield xa

    Resource.make(acquire) { xa =>
      // Fragment.const(WaterFeature.tableDefinitions.head.drop).update.run.transact(xa).as(())
      IO.unit
    }
  }

  test("simple waters from ways") { xa =>
    val actual = WaterFeature
      .dataGenerator
      .map { (key, operation) => operation.transact(xa).map { key -> _ } }
      .sequence

    for
      summary <- actual
    yield expect(summary == List("waters" -> 2414))
  }

  test("Dunger See") { xa =>
    val dungerSee =
      waterRelations
        .stream
        .filter { case (id, _, _, _) => id == 2952 }
        .transact(xa)

    dungerSee.compile.toList
      .map {
        case Nil                         => failure("Cannot find Dunger See")
        case (id, name, kind, tags) :: _ => expect.all(
          id == 2952,
          name == "Dunger See".some
        )
      }
  }

  test("outerComplexPolygon") { xa =>
    for
      list <- outerComplexPolygon(2952).stream.transact(xa).compile.toList
    yield expect.all(
      list.size == 1,
      list.head.numRings() == 1
    )
  }

  // test("insertList") { xa =>
  //   val relationId = 2952
  //   for
  //     data <- waterRelations.stream.filter { case (id, _, _, _) => id == relationId }.transact(xa).compile.toList
  //     geom <- outerComplexPolygon(relationId).stream.transact(xa).compile.toList
  //     list <- insertList(List((data.head._1, data.head._2, data.head._3, geom.head, data.head._4))).transact(xa)
  //   yield expect.all(
  //     list == 1
  //   )
  // }

  test("insert") { xa =>
    for
      data <- waterRelations.stream.filter { case (id, _, _, _) => id == 2952 }.transact(xa).compile.toList
      geom <- outerComplexPolygon(2952).stream.transact(xa).compile.toList
      list <- insertList(List((data.head._1, data.head._2, data.head._3, geom.head, data.head._4))).transact(xa)
    yield expect.all(
      list == 1
    )

    val insertions = insert[IO](xa).foldMonoid.compile.lastOrError
    for
      count <- insertions
    yield expect.all(count == 127)
  }
}
