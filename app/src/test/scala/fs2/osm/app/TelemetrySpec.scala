package fs2.osm.app

import cats.effect.*
import weaver.*

object TelemetrySpec extends SimpleIOSuite {

  test("asdf") {
    for
      telemetry <- Telemetry.apply[IO]
      nodesCount <- telemetry.addCounter("nodes_count", "Total number of nodes")
      _           = nodesCount.add(10)
    yield expect(
      true
    )
  }

}
