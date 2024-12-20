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
    for summary <- WaterFeature.run(xa).compile.toList
    yield expect(summary.head == ("waters", 2418))
  }
}
