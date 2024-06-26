package fs2.osm
package core

import cats.effect.*
import cats.syntax.all.*
import fs2.*
import fs2.io.file.{Files, Path}
import fs2.io.readInputStream
import weaver.*

object PbfReaderSpec extends SimpleIOSuite {
  private val bremen = {
    val inputStream = IO(getClass.getResourceAsStream("/bremen-20240408.osm.pbf"))
    readInputStream(inputStream, 1024, closeAfterUse = true)
  }

  test("correct number of blobs is available") {
    for   count <- PbfReader.pipe(bremen).compile.count
    yield expect(count == 244)
  }

  test("blob and header have correct values") {
    for
      count  <- PbfReader.pipe(bremen).compile.count
      list   <- PbfReader.pipe(bremen).take(3).compile.toList
    yield expect.all(
      count == 244,
      list(0)._1.`type` == "OSMHeader",
      list(0)._1.datasize == 180,

      list(1)._1.`type` == "OSMData",
      list(1)._1.datasize == 74890,
      list(1)._2.raw.isEmpty,
      list(1)._2.rawSize.get == 126978,
      list(1)._2.zlibData.nonEmpty,
      list(1)._2.toPrimitiveBlock.isSuccess
    )
  }

  test("relation Dunger See") {
    val entities = bremen.through(OsmEntityDecoder.pipe)

    for relation <- entities.find(_.osmId == 2952L).compile.toList.map(_.head.asInstanceOf[Relation])
    yield expect.all(
      relation.relations.size == 4,
      relation.relations(0).osmId == 293249683L,
      relation.relations(0).role  == "inner",
      relation.relations(1).osmId == 293249772L,
      relation.relations(1).role  == "inner",
      relation.relations(2).osmId == 11560506L,
      relation.relations(2).role  == "inner",
      relation.relations(3).osmId == 293249767L,
      relation.relations(3).role  == "outer"
    )
  }
}
