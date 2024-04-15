package fs2.osm
package core

import cats.syntax.all.*
import org.openstreetmap.osmosis.osmbinary.osmformat
import weaver.*

object EntityStreamSpec extends SimpleIOSuite {
  pureTest("streams relations") {
    val list = EntityStream(
      osmformat.PrimitiveGroup(relations = Seq(osmformat.Relation(id = 42))),
      osmformat.StringTable()
    ).compile.toList

    expect.all(
      list.size == 1,
      list.head.isInstanceOf[Relation]
    )
  }

  pureTest("streams ways") {
    val list = EntityStream(
      osmformat.PrimitiveGroup(ways = Seq(osmformat.Way(id = 42))),
      osmformat.StringTable()
    ).compile.toList

    expect.all(
      list.size == 1,
      list.head.isInstanceOf[Way]
    )
  }

  pureTest("streams densenodes") {
    val list = EntityStream(
      osmformat.PrimitiveGroup(
        dense = osmformat.DenseNodes(
          id = Seq(42),
          lat = Seq(1),
          lon = Seq(2),
          keysVals = Seq(0)
        ).some
      ),
      osmformat.StringTable()
    ).compile.toList

    expect.all(
      list.size == 1,
      list.head.isInstanceOf[Node]
    )
  }
}
