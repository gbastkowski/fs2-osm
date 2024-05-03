package fs2.osm
package postgres

import cats.effect.*
import cats.implicits.*
import doobie.*
import doobie.implicits.*
import weaver.*

object AdministrativeBoundaryFeatureSpec extends IOSuite {
  import AdministrativeBoundaryFeature.*
  import Schema.*

  override type Res = Transactor[IO]
  override def sharedResource: Resource[IO, Res] = {
    val xa = Config("jdbc:postgresql:fs2-osm", "gunnar.bastkowski", "").transactor

    val acquire =
      for
        _  <- Fragment.const(tableDefinitions.head.drop).update.run.transact(xa)
        _  <- Fragment.const(tableDefinitions.head.create).update.run.transact(xa)
      yield xa

    Resource.make(acquire) { xa =>
      // Fragment.const(tableDefinitions.head.drop).update.run.transact(xa).as(())
      IO.unit
    }
  }

  // test("simple waters from ways") { xa =>
  //   val actual = dataGenerator
  //     .map { (key, operation) => operation.transact(xa).map { key -> _ } }
  //     .sequence

  //   for
  //     summary <- actual
  //   yield expect(summary == List("waters" -> 2409))
  // }


  test("insert") { xa =>
    val insertions = insert[IO](xa).foldMonoid.compile.lastOrError
    for
      count <- insertions
    yield expect.all(count == 201)
  }
}
