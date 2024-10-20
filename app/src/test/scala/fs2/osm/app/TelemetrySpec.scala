package fs2.osm
package app

import cats.effect.*
import fs2.osm.telemetry.{Attributes, Telemetry}
import weaver.*

object TelemetrySpec extends SimpleIOSuite {
  test("asdf") {
    Telemetry[IO](Attributes("asdf", "v", getClass.getName), telemetry.Config.empty).use { telemetry =>
      for
        nodesCount <- telemetry.counter[Long](getClass.getSimpleName, "nodes.count")
        _           = nodesCount.add(10)
      yield expect(true)
    }
  }
}
